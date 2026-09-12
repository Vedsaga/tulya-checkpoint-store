use std::error::Error;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use serde_json::{json, Value};
use tulya_core::persistent_history::authority::{open_history_authority, WritableHistoryAuthority};
use tulya_core::persistent_history::{CommitOutcome, Version, VersionId};

const BENCHMARK: &str = "TULYA_LARGE_BRANCHING_STATE_V1";
const NODE_HEADER_BYTES: u64 = 16;
const NODE_RECORD_BYTES: u64 = 72;
const READ4K: usize = 4096;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)] db: PathBuf,
    #[arg(long)] base: PathBuf,
    #[arg(long)] history: PathBuf,
    #[arg(long)] edits: PathBuf,
    #[arg(long, default_value_t = 128)] verify_samples: usize,
    #[arg(long, default_value_t = false)] fresh: bool,
}

#[derive(Clone, Copy, Debug)]
struct Op { version: u64, parent: u64, offset: u64, length: u64 }

fn parse_history(path: &Path, base_len: u64) -> Result<Vec<Op>, Box<dyn Error>> {
    let mut ops = Vec::new();
    for (line_no, line) in BufReader::new(File::open(path)?).lines().enumerate() {
        let row: Value = serde_json::from_str(&line?)
            .map_err(|e| format!("invalid history line {}: {e}", line_no + 1))?;
        let get = |name: &str| row.get(name).and_then(Value::as_u64).ok_or_else(|| format!("history row missing {name}"));
        let op = Op { version: get("version")?, parent: get("parent")?, offset: get("offset")?, length: get("length")? };
        let expected = u64::try_from(ops.len())? + 1;
        if op.version != expected || op.parent >= op.version { return Err("history topology is not dense/prior-parent".into()); }
        if op.length == 0 || op.offset.checked_add(op.length).ok_or("edit range overflow")? > base_len { return Err("edit range exceeds base".into()); }
        ops.push(op);
    }
    Ok(ops)
}

fn committed(outcome: CommitOutcome) -> Result<Version, Box<dyn Error>> {
    outcome.version().ok_or_else(|| "benchmark commit produced no version".into())
}

fn pct(values: &[u128], p: usize) -> u128 {
    if values.is_empty() { return 0; }
    let mut v = values.to_vec(); v.sort_unstable(); v[(v.len() - 1) * p / 100]
}
fn latency(values: &[u128]) -> Value {
    json!({"count": values.len(), "p50_ns": pct(values,50), "p95_ns": pct(values,95), "p99_ns": pct(values,99), "max_ns": values.iter().copied().max().unwrap_or(0)})
}
fn node_stats(values: &[u64]) -> Value {
    let mut v = values.to_vec(); v.sort_unstable();
    let p = |n: usize| if v.is_empty() {0} else {v[(v.len()-1)*n/100]};
    let total: u64 = values.iter().copied().sum();
    json!({"count":values.len(),"total":total,"mean":if values.is_empty(){0.0}else{total as f64/values.len() as f64},"min":v.first().copied().unwrap_or(0),"p50":p(50),"p95":p(95),"p99":p(99),"max":v.last().copied().unwrap_or(0)})
}

fn node_file_bytes(root: &Path) -> Result<u64, Box<dyn Error>> {
    let mut total = 0u64;
    for e in fs::read_dir(root)? {
        let e = e?; let m = e.metadata()?; if !m.is_file() { continue; }
        let n = e.file_name(); let n = n.to_string_lossy();
        if n.starts_with("content-") && n.ends_with(".nodes") { total = total.checked_add(m.len()).ok_or("node bytes overflow")?; }
    }
    Ok(total)
}
fn node_records(bytes: u64) -> Result<u64, Box<dyn Error>> {
    if bytes < NODE_HEADER_BYTES || (bytes - NODE_HEADER_BYTES) % NODE_RECORD_BYTES != 0 { return Err("node file alignment invalid".into()); }
    Ok((bytes - NODE_HEADER_BYTES) / NODE_RECORD_BYTES)
}

fn storage_breakdown(root: &Path) -> Result<Value, Box<dyn Error>> {
    let mut total=0u64; let mut payload=0u64; let mut nodes=0u64; let mut wal=0u64; let mut snapshot=0u64; let mut manifest=0u64; let mut other=0u64; let mut files=Vec::new();
    for e in fs::read_dir(root)? {
        let e=e?; let m=e.metadata()?; if !m.is_file(){continue;} let bytes=m.len(); total+=bytes;
        let name=e.file_name().to_string_lossy().into_owned();
        let category = if name.starts_with("content-")&&name.ends_with(".payload") {payload+=bytes;"payload"}
            else if name.starts_with("content-")&&name.ends_with(".nodes") {nodes+=bytes;"nodes"}
            else if name.starts_with("history-snap-")&&name.ends_with(".ths") {snapshot+=bytes;"snapshot"}
            else if name.starts_with("history-")&&name.ends_with(".wal") {wal+=bytes;"wal"}
            else if name=="history-manifest.json" {manifest+=bytes;"manifest"}
            else if name=="history.lock" {"lock"} else {other+=bytes;"other"};
        files.push(json!({"path":name,"bytes":bytes,"category":category}));
    }
    Ok(json!({"total_bytes":total,"payload_file_bytes":payload,"node_file_bytes":nodes,"wal_file_bytes":wal,"snapshot_file_bytes":snapshot,"manifest_file_bytes":manifest,"other_file_bytes":other,"files":files}))
}

fn read_edit(file: &mut File, version: u64, length: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    let offset = (version - 1).checked_mul(u64::try_from(length)?).ok_or("edit corpus offset overflow")?;
    file.seek(SeekFrom::Start(offset))?;
    let mut out=vec![0u8;length]; file.read_exact(&mut out)?; Ok(out)
}

fn sample_versions(updates: usize, samples: usize) -> Vec<u64> {
    let count=samples.max(1).min(updates); if count==1{return vec![u64::try_from(updates).unwrap_or(1)];}
    (0..count).map(|i| u64::try_from(1 + i*(updates-1)/(count-1)).unwrap_or(1)).collect()
}

fn run(args: &Args) -> Result<Value, Box<dyn Error>> {
    if args.fresh && args.db.exists(){fs::remove_dir_all(&args.db)?;} fs::create_dir_all(&args.db)?;
    let base=fs::read(&args.base)?; if base.is_empty(){return Err("empty base".into());}
    let base_len=u64::try_from(base.len())?; let base_bytes=base.len();
    let ops=parse_history(&args.history,base_len)?; if ops.is_empty(){return Err("empty history".into());}
    let edit_bytes=usize::try_from(ops[0].length)?; if ops.iter().any(|o| usize::try_from(o.length).ok()!=Some(edit_bytes)){return Err("benchmark requires fixed edit length".into());}
    let expected_edits=u64::try_from(ops.len())?.checked_mul(u64::try_from(edit_bytes)?).ok_or("edit corpus size overflow")?;
    if fs::metadata(&args.edits)?.len()!=expected_edits{return Err("edit corpus length mismatch".into());}

    let t=Instant::now(); let mut authority=WritableHistoryAuthority::open(&args.db)?; let open_ns=t.elapsed().as_nanos();
    let history_id=authority.create_history(None)?;
    let t=Instant::now(); let root=committed(authority.splice(history_id,None,0,0,&base,None,None)?)?; let root_create_ns=t.elapsed().as_nanos();
    drop(base);
    let initial_node_file_bytes=node_file_bytes(&args.db)?; let initial_node_records=node_records(initial_node_file_bytes)?; let mut prev=initial_node_records;
    let mut versions=Vec::with_capacity(ops.len()+1); versions.push(root);
    let mut update_times=Vec::with_capacity(ops.len()); let mut allocations=Vec::with_capacity(ops.len()); let mut edits=File::open(&args.edits)?;
    for op in &ops {
        let parent=*versions.get(usize::try_from(op.parent)?).ok_or("missing parent")?;
        let payload=read_edit(&mut edits,op.version,edit_bytes)?;
        let t=Instant::now(); let child=committed(authority.splice(history_id,Some(parent.id()),op.offset,op.length,&payload,None,None)?)?; update_times.push(t.elapsed().as_nanos());
        if child.id().id()!=op.version{return Err("Tulya version id mismatch".into());} versions.push(child);
        let now=node_records(node_file_bytes(&args.db)?)?; allocations.push(now.checked_sub(prev).ok_or("node count regressed")?); prev=now;
    }
    let final_node_file_bytes=node_file_bytes(&args.db)?; let final_node_records=node_records(final_node_file_bytes)?; let fresh=final_node_records-initial_node_records;
    if allocations.iter().copied().sum::<u64>()!=fresh{return Err("node allocation accounting mismatch".into());}
    let pre=storage_breakdown(&args.db)?; let pre_bytes=pre["total_bytes"].as_u64().ok_or("bad pre total")?;
    let t=Instant::now(); let seal=authority.seal()?; let seal_ns=t.elapsed().as_nanos();
    let post=storage_breakdown(&args.db)?; let post_bytes=post["total_bytes"].as_u64().ok_or("bad post total")?; drop(authority);
    let t=Instant::now(); let reopened=open_history_authority(&args.db)?; let reopen_ns=t.elapsed().as_nanos();

    let mut failures=0u64; let mut read4=Vec::new(); let mut read_edit_times=Vec::new(); let mut edits=File::open(&args.edits)?;
    for version_id in sample_versions(ops.len(),args.verify_samples) {
        let op=ops[usize::try_from(version_id-1)?]; let expected=read_edit(&mut edits,version_id,edit_bytes)?;
        let version=reopened.store.lookup_version(VersionId::new(version_id))?;
        let t=Instant::now(); let mut small=Vec::with_capacity(READ4K); reopened.store.read(version,op.offset,u64::try_from(READ4K)?,&mut small)?; read4.push(t.elapsed().as_nanos());
        let t=Instant::now(); let mut whole=Vec::with_capacity(edit_bytes); reopened.store.read(version,op.offset,op.length,&mut whole)?; read_edit_times.push(t.elapsed().as_nanos());
        failures += u64::from(small != expected[..READ4K]) + u64::from(whole != expected);
    }

    Ok(json!({
        "benchmark":BENCHMARK,"backend":"tulya-balanced-durable","base_bytes":base_bytes,"edit_bytes":edit_bytes,"updates":ops.len(),"versions":ops.len()+1,"exact":failures==0,
        "verification":{"sample_count":sample_versions(ops.len(),args.verify_samples).len(),"failures":failures},
        "storage":{"pre_seal_file_bytes":pre_bytes,"post_seal_file_bytes":post_bytes,"pre_seal_breakdown":pre,"post_seal_breakdown":post},
        "node_allocation":{"record_size_bytes":NODE_RECORD_BYTES,"file_header_bytes":NODE_HEADER_BYTES,"initial_node_records":initial_node_records,"final_node_records":final_node_records,"fresh_node_records_total":fresh,"fresh_node_bytes_total":fresh*NODE_RECORD_BYTES,"per_update_fresh_node_records":node_stats(&allocations)},
        "latency":{"open_ns":open_ns,"root_create_ns":root_create_ns,"update_splice":latency(&update_times),"seal_ns":seal_ns,"reopen_ns":reopen_ns,"historical_read_4k":latency(&read4),"historical_read_edit":latency(&read_edit_times)},
        "seal":{"generation":seal.generation,"snapshot_len":seal.snapshot_len,"represented_wal_end":seal.represented_wal_end,"recycled_hot":seal.recycled_hot},
        "reopen":{"generation":reopened.generation,"snapshot_versions":reopened.stats.snapshot_versions,"suffix_bytes":reopened.stats.suffix_bytes}
    }))
}

fn main()->Result<(),Box<dyn Error>>{let args=Args::parse();println!("{}",serde_json::to_string_pretty(&run(&args)?)?);Ok(())}
