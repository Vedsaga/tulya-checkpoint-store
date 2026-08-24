//! Loopback-only alpha HTTP service and dashboard for evaluating Tulya locally.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Cursor, Read};
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use serde_json::{json, Map, Value};
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};
use tulya_checkpoint_store::{
    CheckpointStore, CheckpointStoreConfig, CheckpointStoreError, HotWalAppendReport,
};

const DASHBOARD_HTML: &str = include_str!("tulya_dashboard.html");
const MAX_JSON_BODY_BYTES: u64 = 8 * 1024 * 1024;
const MAX_IMPORT_BODY_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(
    name = "tulya-local",
    about = "Local alpha API and dashboard for trying append-only checkpoint histories"
)]
struct Args {
    /// Tulya store directory.
    #[arg(long)]
    db: PathBuf,
    /// HTTP address. Non-loopback addresses require --allow-non-loopback.
    #[arg(long, default_value = "127.0.0.1:3210")]
    bind: String,
    /// Import append-delta JSONL before starting the server. The store must be empty.
    #[arg(long = "import-jsonl")]
    import_jsonl: Option<PathBuf>,
    /// Explicitly permit an unauthenticated bind outside loopback.
    #[arg(long)]
    allow_non_loopback: bool,
}

#[derive(Debug)]
struct AppendInput {
    thread_id: String,
    checkpoint_id: String,
    checkpoint_no: u32,
    parent_checkpoint_id: Option<String>,
    messages: Vec<Value>,
}

#[derive(Debug, Default)]
struct ImportReport {
    checkpoint_count: u64,
    message_value_count: u64,
    input_bytes: u64,
}

#[derive(Debug)]
struct ServiceMetrics {
    started: Instant,
    requests_total: u64,
    errors_total: u64,
    append_requests_total: u64,
    read_requests_total: u64,
    import_requests_total: u64,
    imported_checkpoints_total: u64,
    accepted_message_values_total: u64,
    accepted_message_json_bytes_total: u64,
    last_append_write_ns: u64,
    last_append_sync_data_ns: u64,
}

impl ServiceMetrics {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            requests_total: 0,
            errors_total: 0,
            append_requests_total: 0,
            read_requests_total: 0,
            import_requests_total: 0,
            imported_checkpoints_total: 0,
            accepted_message_values_total: 0,
            accepted_message_json_bytes_total: 0,
            last_append_write_ns: 0,
            last_append_sync_data_ns: 0,
        }
    }

    fn record_append(&mut self, input: &AppendInput, report: HotWalAppendReport) {
        self.append_requests_total = self.append_requests_total.saturating_add(1);
        self.accepted_message_values_total = self
            .accepted_message_values_total
            .saturating_add(u64::try_from(input.messages.len()).unwrap_or(u64::MAX));
        self.accepted_message_json_bytes_total =
            self.accepted_message_json_bytes_total.saturating_add(
                u64::try_from(serde_json::to_vec(&input.messages).map_or(0, |body| body.len()))
                    .unwrap_or(u64::MAX),
            );
        self.last_append_write_ns = u64::try_from(report.write_ns).unwrap_or(u64::MAX);
        self.last_append_sync_data_ns = u64::try_from(report.sync_data_ns).unwrap_or(u64::MAX);
    }
}

#[derive(Debug)]
struct ApiError {
    status: u16,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: 400,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: 404,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: 500,
            message: message.into(),
        }
    }
}

struct Reply {
    status: u16,
    content_type: &'static str,
    body: String,
}

impl Reply {
    fn json(status: u16, value: Value) -> Self {
        Self {
            status,
            content_type: "application/json; charset=utf-8",
            body: serde_json::to_string_pretty(&value).unwrap_or_else(|_| {
                "{\"ok\":false,\"error\":\"response encoding failed\"}".to_owned()
            }),
        }
    }

    fn text(status: u16, content_type: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type,
            body: body.into(),
        }
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("tulya-local: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    if !args.allow_non_loopback && !is_loopback_bind(&args.bind) {
        return Err(format!(
            "refusing unauthenticated non-loopback bind {}; use --allow-non-loopback only on a trusted network",
            args.bind
        )
        .into());
    }

    let mut store = CheckpointStore::open(&args.db, CheckpointStoreConfig::default())?;
    let mut metrics = ServiceMetrics::new();
    if let Some(path) = args.import_jsonl.as_ref() {
        if store.checkpoint_count() != 0 {
            return Err("--import-jsonl requires an empty Tulya store".into());
        }
        let source = BufReader::new(File::open(path)?);
        let report = import_jsonl(&mut store, source, &mut metrics)?;
        println!(
            "Imported {} checkpoints and {} message values from {}",
            report.checkpoint_count,
            report.message_value_count,
            path.display()
        );
    }

    let server = Server::http(&args.bind)
        .map_err(|error| io::Error::other(format!("HTTP bind failed: {error}")))?;
    println!("Tulya local dashboard: http://{}", args.bind);
    println!("JSON API:             http://{}/api/health", args.bind);
    println!("Prometheus metrics:   http://{}/metrics", args.bind);
    println!("Alpha local evaluator: no authentication, TLS, or multi-writer support.");

    for request in server.incoming_requests() {
        handle_request(request, &mut store, &mut metrics);
    }
    Ok(())
}

fn handle_request(mut request: Request, store: &mut CheckpointStore, metrics: &mut ServiceMetrics) {
    metrics.requests_total = metrics.requests_total.saturating_add(1);
    let reply = match route(&mut request, store, metrics) {
        Ok(reply) => reply,
        Err(error) => {
            metrics.errors_total = metrics.errors_total.saturating_add(1);
            Reply::json(error.status, json!({"ok": false, "error": error.message}))
        }
    };

    let mut response = Response::from_string(reply.body).with_status_code(StatusCode(reply.status));
    response.add_header(header("Content-Type", reply.content_type));
    response.add_header(header("Cache-Control", "no-store"));
    if let Err(error) = request.respond(response) {
        metrics.errors_total = metrics.errors_total.saturating_add(1);
        eprintln!("tulya-local response error: {error}");
    }
}

fn route(
    request: &mut Request,
    store: &mut CheckpointStore,
    metrics: &mut ServiceMetrics,
) -> Result<Reply, ApiError> {
    let method = request.method().clone();
    let url = request.url().to_owned();
    let path = url.split('?').next().unwrap_or(&url);
    match (method, path) {
        (Method::Get, "/") => Ok(Reply::text(200, "text/html; charset=utf-8", DASHBOARD_HTML)),
        (Method::Get, "/api/health") => Ok(Reply::json(
            200,
            json!({
                "ok": true,
                "status": "alpha-local-evaluator",
                "checkpoint_count": store.checkpoint_count(),
                "limitations": ["no authentication", "no TLS", "single process", "single writer"]
            }),
        )),
        (Method::Get, "/api/stats") => Ok(Reply::json(200, store_stats(store, metrics)?)),
        (Method::Get, "/api/checkpoints") => {
            let limit = query_limit(&url);
            Ok(Reply::json(200, checkpoint_list(store, limit)))
        }
        (Method::Get, "/metrics") => Ok(Reply::text(
            200,
            "text/plain; version=0.0.4; charset=utf-8",
            prometheus_metrics(store, metrics)?,
        )),
        (Method::Post, "/api/checkpoints") => {
            let body = read_body_limited(request, MAX_JSON_BODY_BYTES)?;
            let value: Value = serde_json::from_slice(&body)
                .map_err(|error| ApiError::bad_request(format!("invalid JSON: {error}")))?;
            let input = parse_append_input(value)?;
            let report = append_one(store, &input)?;
            metrics.record_append(&input, report);
            Ok(Reply::json(
                201,
                json!({
                    "ok": true,
                    "operation": "append",
                    "thread_id": input.thread_id,
                    "checkpoint_id": input.checkpoint_id,
                    "checkpoint_count": store.checkpoint_count(),
                    "transaction_bytes": report.transaction_bytes,
                    "write_ns": report.write_ns,
                    "sync_data_ns": report.sync_data_ns
                }),
            ))
        }
        (Method::Post, "/api/read") => {
            let body = read_body_limited(request, MAX_JSON_BODY_BYTES)?;
            let value: Value = serde_json::from_slice(&body)
                .map_err(|error| ApiError::bad_request(format!("invalid JSON: {error}")))?;
            let (thread_id, checkpoint_id) = parse_read_input(value)?;
            let state = store
                .read_checkpoint(&thread_id, &checkpoint_id)
                .map_err(read_store_error)?;
            let state: Value = serde_json::from_slice(&state)
                .map_err(|error| ApiError::internal(error.to_string()))?;
            metrics.read_requests_total = metrics.read_requests_total.saturating_add(1);
            Ok(Reply::json(
                200,
                json!({
                    "ok": true,
                    "thread_id": thread_id,
                    "checkpoint_id": checkpoint_id,
                    "state": state
                }),
            ))
        }
        (Method::Post, "/api/import") => {
            let body = read_body_limited(request, MAX_IMPORT_BODY_BYTES)?;
            metrics.import_requests_total = metrics.import_requests_total.saturating_add(1);
            let report = import_jsonl(store, BufReader::new(Cursor::new(body)), metrics)
                .map_err(ApiError::bad_request)?;
            Ok(Reply::json(
                200,
                json!({
                    "ok": true,
                    "operation": "import",
                    "imported_checkpoints": report.checkpoint_count,
                    "imported_message_values": report.message_value_count,
                    "input_bytes": report.input_bytes,
                    "checkpoint_count": store.checkpoint_count()
                }),
            ))
        }
        (Method::Post, "/api/verify") => {
            let report = store
                .verify_all()
                .map_err(|error| ApiError::internal(error.to_string()))?;
            Ok(Reply::json(
                if report.failures == 0 { 200 } else { 500 },
                json!({
                    "ok": report.failures == 0,
                    "operation": "verify",
                    "checkpoint_count": report.checkpoint_count,
                    "failures": report.failures
                }),
            ))
        }
        (Method::Post, "/api/seal") => {
            let target = u64::try_from(store.checkpoint_count())
                .map_err(|_| ApiError::internal("checkpoint count exceeds u64"))?;
            let report = store
                .seal_through(target)
                .map_err(|error| ApiError::internal(error.to_string()))?;
            Ok(Reply::json(
                200,
                json!({
                    "ok": true,
                    "operation": "seal",
                    "generation": report.generation,
                    "checkpoint_count": report.checkpoint_count,
                    "reclaimed_allocated_bytes": report.reclaimed.allocated_bytes
                }),
            ))
        }
        _ => Err(ApiError::not_found(format!(
            "no route for {} {}",
            request.method(),
            path
        ))),
    }
}

fn append_one(
    store: &mut CheckpointStore,
    input: &AppendInput,
) -> Result<HotWalAppendReport, ApiError> {
    store
        .append_messages_checkpoint(
            &input.thread_id,
            &input.checkpoint_id,
            input.checkpoint_no,
            input.parent_checkpoint_id.as_deref(),
            &input.messages,
        )
        .map_err(append_store_error)
}

fn append_store_error(error: CheckpointStoreError) -> ApiError {
    match error {
        CheckpointStoreError::Io(_) | CheckpointStoreError::Json(_) => {
            ApiError::internal(error.to_string())
        }
        _ => ApiError::bad_request(error.to_string()),
    }
}

fn read_store_error(error: CheckpointStoreError) -> ApiError {
    match error {
        CheckpointStoreError::CheckpointNotFound | CheckpointStoreError::CheckpointDeleted => {
            ApiError::not_found(error.to_string())
        }
        _ => ApiError::internal(error.to_string()),
    }
}

fn parse_append_input(value: Value) -> Result<AppendInput, ApiError> {
    let mut object = value
        .as_object()
        .cloned()
        .ok_or_else(|| ApiError::bad_request("checkpoint body must be a JSON object"))?;
    reject_unknown_fields(
        &object,
        &[
            "thread_id",
            "checkpoint_id",
            "checkpoint_no",
            "parent_checkpoint_id",
            "messages",
        ],
    )?;
    let thread_id = take_string(&mut object, "thread_id")?;
    let checkpoint_id = take_string(&mut object, "checkpoint_id")?;
    let checkpoint_no = u32::try_from(take_u64(&mut object, "checkpoint_no")?)
        .map_err(|_| ApiError::bad_request("checkpoint_no exceeds u32"))?;
    let parent_checkpoint_id = match object.remove("parent_checkpoint_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value),
        Some(_) => {
            return Err(ApiError::bad_request(
                "parent_checkpoint_id must be a string or null",
            ))
        }
    };
    let messages = object
        .remove("messages")
        .and_then(|value| value.as_array().cloned())
        .ok_or_else(|| ApiError::bad_request("messages must be a JSON array"))?;
    if messages.is_empty() {
        return Err(ApiError::bad_request(
            "messages must contain at least one newly appended value",
        ));
    }
    Ok(AppendInput {
        thread_id,
        checkpoint_id,
        checkpoint_no,
        parent_checkpoint_id,
        messages,
    })
}

fn parse_read_input(value: Value) -> Result<(String, String), ApiError> {
    let mut object = value
        .as_object()
        .cloned()
        .ok_or_else(|| ApiError::bad_request("read body must be a JSON object"))?;
    reject_unknown_fields(&object, &["thread_id", "checkpoint_id"])?;
    let thread_id = take_string(&mut object, "thread_id")?;
    let checkpoint_id = take_string(&mut object, "checkpoint_id")?;
    Ok((thread_id, checkpoint_id))
}

fn reject_unknown_fields(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), ApiError> {
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(ApiError::bad_request(format!(
            "unknown field {field:?}; expected only {}",
            allowed.join(", ")
        )));
    }
    Ok(())
}

fn take_string(object: &mut Map<String, Value>, field: &str) -> Result<String, ApiError> {
    match object.remove(field) {
        Some(Value::String(value)) if !value.is_empty() => Ok(value),
        _ => Err(ApiError::bad_request(format!(
            "{field} must be a non-empty string"
        ))),
    }
}

fn take_u64(object: &mut Map<String, Value>, field: &str) -> Result<u64, ApiError> {
    object
        .remove(field)
        .and_then(|value| value.as_u64())
        .ok_or_else(|| ApiError::bad_request(format!("{field} must be an unsigned integer")))
}

fn import_jsonl<R: BufRead>(
    store: &mut CheckpointStore,
    source: R,
    metrics: &mut ServiceMetrics,
) -> Result<ImportReport, String> {
    let mut imported = ImportReport::default();
    for (index, line) in source.lines().enumerate() {
        let line_no = index + 1;
        let line = line.map_err(|error| {
            format!(
                "import stopped at line {line_no} after {} durable checkpoints: {error}",
                imported.checkpoint_count
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        imported.input_bytes = imported
            .input_bytes
            .saturating_add(u64::try_from(line.len()).unwrap_or(u64::MAX));
        let value: Value = serde_json::from_str(&line).map_err(|error| {
            format!(
                "import stopped at line {line_no} after {} durable checkpoints: invalid JSON: {error}",
                imported.checkpoint_count
            )
        })?;
        let input = parse_append_input(value).map_err(|error| {
            format!(
                "import stopped at line {line_no} after {} durable checkpoints: {}",
                imported.checkpoint_count, error.message
            )
        })?;
        let report = append_one(store, &input).map_err(|error| {
            format!(
                "import stopped at line {line_no} after {} durable checkpoints: {}",
                imported.checkpoint_count, error.message
            )
        })?;
        imported.checkpoint_count = imported.checkpoint_count.saturating_add(1);
        imported.message_value_count = imported
            .message_value_count
            .saturating_add(u64::try_from(input.messages.len()).unwrap_or(u64::MAX));
        metrics.record_append(&input, report);
        metrics.imported_checkpoints_total = metrics.imported_checkpoints_total.saturating_add(1);
    }
    let verification = store.verify_all().map_err(|error| {
        format!(
            "imported {} durable checkpoints but verification failed: {error}",
            imported.checkpoint_count
        )
    })?;
    if verification.failures != 0 {
        return Err(format!(
            "imported {} durable checkpoints but exact verification found {} failures",
            imported.checkpoint_count, verification.failures
        ));
    }
    Ok(imported)
}

fn store_stats(store: &CheckpointStore, metrics: &ServiceMetrics) -> Result<Value, ApiError> {
    let storage = store
        .storage()
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let hot_capacity_bytes = store
        .hot_capacity_bytes()
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let geometry = workload_geometry(store);
    Ok(json!({
        "ok": true,
        "status": "alpha-local-evaluator",
        "store": {
            "checkpoint_count": store.checkpoint_count(),
            "version_count": store.version_count(),
            "sealed_checkpoint_count": store.sealed_checkpoint_count(),
            "hot_logical_bytes": store.hot_logical_bytes(),
            "hot_capacity_bytes": hot_capacity_bytes,
            "file_length_bytes": storage.file_length_bytes,
            "allocated_bytes": storage.allocated_bytes,
            "file_count": storage.file_count
        },
        "workload": geometry,
        "service": {
            "uptime_seconds": metrics.started.elapsed().as_secs(),
            "requests_total": metrics.requests_total,
            "errors_total": metrics.errors_total,
            "append_requests_total": metrics.append_requests_total,
            "read_requests_total": metrics.read_requests_total,
            "import_requests_total": metrics.import_requests_total,
            "imported_checkpoints_total": metrics.imported_checkpoints_total,
            "accepted_message_values_total": metrics.accepted_message_values_total,
            "accepted_message_json_bytes_total": metrics.accepted_message_json_bytes_total,
            "last_append_write_ns": metrics.last_append_write_ns,
            "last_append_sync_data_ns": metrics.last_append_sync_data_ns
        },
        "limitations": [
            "local evaluator; not a production network service",
            "no authentication or TLS",
            "single process and single writer",
            "append-only messages only",
            "metrics reset when the service restarts"
        ]
    }))
}

fn workload_geometry(store: &CheckpointStore) -> Value {
    let mut threads = HashSet::new();
    let mut parents = HashSet::new();
    let mut fanout: HashMap<(String, String), u64> = HashMap::new();
    let mut parent_by_checkpoint = HashMap::new();
    let mut roots = 0_u64;
    let mut logical_snapshot_bytes = 0_u64;

    for checkpoint in store.checkpoints() {
        threads.insert(checkpoint.thread_id.clone());
        logical_snapshot_bytes =
            logical_snapshot_bytes.saturating_add(checkpoint.logical_state_len);
        let key = (
            checkpoint.thread_id.clone(),
            checkpoint.checkpoint_id.clone(),
        );
        let parent = checkpoint.parent_checkpoint_id.as_ref().map(|parent| {
            let parent_key = (checkpoint.thread_id.clone(), parent.clone());
            parents.insert(parent_key.clone());
            *fanout.entry(parent_key.clone()).or_insert(0) += 1;
            parent_key
        });
        if parent.is_none() {
            roots = roots.saturating_add(1);
        }
        parent_by_checkpoint.insert(key, parent);
    }

    let mut max_depth = 0_u64;
    for key in parent_by_checkpoint.keys() {
        let mut depth = 0_u64;
        let mut cursor = Some(key.clone());
        let mut seen = HashSet::new();
        while let Some(current) = cursor {
            if !seen.insert(current.clone()) {
                break;
            }
            depth = depth.saturating_add(1);
            cursor = parent_by_checkpoint.get(&current).cloned().flatten();
        }
        max_depth = max_depth.max(depth);
    }

    json!({
        "thread_count": threads.len(),
        "root_count": roots,
        "leaf_count": store.checkpoint_count().saturating_sub(parents.len()),
        "branch_point_count": fanout.values().filter(|children| **children > 1).count(),
        "maximum_fanout": fanout.values().copied().max().unwrap_or(0),
        "maximum_depth": max_depth,
        "sum_of_checkpoint_state_bytes": logical_snapshot_bytes
    })
}

fn checkpoint_list(store: &CheckpointStore, limit: usize) -> Value {
    let start = store.checkpoint_count().saturating_sub(limit);
    let checkpoints = store.checkpoints()[start..]
        .iter()
        .map(|checkpoint| {
            json!({
                "ordinal": checkpoint.ordinal,
                "thread_id": checkpoint.thread_id,
                "checkpoint_no": checkpoint.checkpoint_no,
                "checkpoint_id": checkpoint.checkpoint_id,
                "parent_checkpoint_id": checkpoint.parent_checkpoint_id,
                "logical_state_len": checkpoint.logical_state_len,
                "state_hash_xxh3_64_hex": format!("{:016x}", checkpoint.state_hash)
            })
        })
        .collect::<Vec<_>>();
    json!({"ok": true, "limit": limit, "checkpoints": checkpoints})
}

fn prometheus_metrics(
    store: &CheckpointStore,
    metrics: &ServiceMetrics,
) -> Result<String, ApiError> {
    let storage = store
        .storage()
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let geometry = workload_geometry(store);
    let workload = geometry
        .as_object()
        .ok_or_else(|| ApiError::internal("workload geometry is not an object"))?;
    let metric = |name: &str| workload.get(name).and_then(Value::as_u64).unwrap_or(0);
    let mut output = String::new();
    for (name, help, value) in [
        (
            "tulya_checkpoints",
            "Current committed checkpoints.",
            u64::try_from(store.checkpoint_count()).unwrap_or(u64::MAX),
        ),
        (
            "tulya_versions",
            "Current internal version roots.",
            u64::try_from(store.version_count()).unwrap_or(u64::MAX),
        ),
        (
            "tulya_sealed_checkpoints",
            "Checkpoints represented by the current sealed generation.",
            store.sealed_checkpoint_count(),
        ),
        (
            "tulya_hot_logical_bytes",
            "Committed logical bytes in the hot WAL suffix.",
            store.hot_logical_bytes(),
        ),
        (
            "tulya_file_length_bytes",
            "Sum of regular-file lengths in the complete store directory.",
            storage.file_length_bytes,
        ),
        (
            "tulya_allocated_bytes",
            "Filesystem-allocated bytes for the complete store directory.",
            storage.allocated_bytes,
        ),
        (
            "tulya_store_files",
            "Regular files counted in the complete store directory.",
            storage.file_count,
        ),
        (
            "tulya_threads",
            "Distinct logical history threads.",
            metric("thread_count"),
        ),
        (
            "tulya_roots",
            "Root checkpoints without a parent.",
            metric("root_count"),
        ),
        (
            "tulya_leaves",
            "Checkpoints without a direct child.",
            metric("leaf_count"),
        ),
        (
            "tulya_branch_points",
            "Checkpoints with more than one direct child.",
            metric("branch_point_count"),
        ),
        (
            "tulya_maximum_fanout",
            "Largest number of direct children for one checkpoint.",
            metric("maximum_fanout"),
        ),
        (
            "tulya_maximum_depth",
            "Largest checkpoint depth including the root.",
            metric("maximum_depth"),
        ),
        (
            "tulya_sum_checkpoint_state_bytes",
            "Sum of exact logical state lengths across checkpoints.",
            metric("sum_of_checkpoint_state_bytes"),
        ),
        (
            "tulya_service_uptime_seconds",
            "Local evaluator process uptime in seconds.",
            metrics.started.elapsed().as_secs(),
        ),
        (
            "tulya_last_append_write_nanoseconds",
            "Write duration before sync_data for the last accepted append.",
            metrics.last_append_write_ns,
        ),
        (
            "tulya_last_append_sync_data_nanoseconds",
            "sync_data duration for the last accepted append.",
            metrics.last_append_sync_data_ns,
        ),
    ] {
        push_prometheus_metric(&mut output, name, "gauge", help, value);
    }
    for (name, help, value) in [
        (
            "tulya_http_requests_total",
            "HTTP requests handled since process start.",
            metrics.requests_total,
        ),
        (
            "tulya_http_errors_total",
            "HTTP error responses or response failures since process start.",
            metrics.errors_total,
        ),
        (
            "tulya_durable_appends_total",
            "Durable checkpoint appends accepted since process start.",
            metrics.append_requests_total,
        ),
        (
            "tulya_read_requests_total",
            "Exact checkpoint reads served since process start.",
            metrics.read_requests_total,
        ),
        (
            "tulya_import_requests_total",
            "HTTP JSONL import requests since process start.",
            metrics.import_requests_total,
        ),
        (
            "tulya_imported_checkpoints_total",
            "Checkpoints accepted through JSONL import since process start.",
            metrics.imported_checkpoints_total,
        ),
        (
            "tulya_accepted_message_values_total",
            "Appended JSON message values accepted since process start.",
            metrics.accepted_message_values_total,
        ),
        (
            "tulya_accepted_message_json_bytes_total",
            "Serialized JSON bytes in accepted message arrays since process start.",
            metrics.accepted_message_json_bytes_total,
        ),
    ] {
        push_prometheus_metric(&mut output, name, "counter", help, value);
    }
    Ok(output)
}

fn push_prometheus_metric(output: &mut String, name: &str, kind: &str, help: &str, value: u64) {
    writeln!(output, "# HELP {name} {help}").expect("writing to a String cannot fail");
    writeln!(output, "# TYPE {name} {kind}").expect("writing to a String cannot fail");
    writeln!(output, "{name} {value}").expect("writing to a String cannot fail");
}

fn read_body_limited(request: &mut Request, limit: u64) -> Result<Vec<u8>, ApiError> {
    let mut body = Vec::new();
    request
        .as_reader()
        .take(limit.saturating_add(1))
        .read_to_end(&mut body)
        .map_err(|error| ApiError::bad_request(format!("request body read failed: {error}")))?;
    if u64::try_from(body.len()).unwrap_or(u64::MAX) > limit {
        return Err(ApiError {
            status: 413,
            message: format!("request body exceeds {limit} bytes"),
        });
    }
    Ok(body)
}

fn query_limit(url: &str) -> usize {
    url.split_once('?')
        .map(|(_, query)| query)
        .into_iter()
        .flat_map(|query| query.split('&'))
        .find_map(|part| {
            let (key, value) = part.split_once('=')?;
            (key == "limit")
                .then(|| value.parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(100)
        .clamp(1, 1_000)
}

fn is_loopback_bind(bind: &str) -> bool {
    bind.starts_with("127.") || bind.starts_with("localhost:") || bind.starts_with("[::1]:")
}

fn header(name: &str, value: &str) -> Header {
    Header::from_bytes(name.as_bytes(), value.as_bytes()).expect("static HTTP header must be valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> Result<(tempfile::TempDir, CheckpointStore), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let config = CheckpointStoreConfig {
            wal_segment_bytes: 64 * 1024,
            preinit_chunk_bytes: 16 * 1024,
            sealed_block_size: 4 * 1024,
            ..CheckpointStoreConfig::default()
        };
        let store = CheckpointStore::open(dir.path(), config)?;
        Ok((dir, store))
    }

    #[test]
    fn append_schema_rejects_hidden_full_state_and_unknown_fields() {
        let error = parse_append_input(json!({
            "thread_id": "t",
            "checkpoint_id": "c",
            "checkpoint_no": 0,
            "parent_checkpoint_id": null,
            "state": {"messages": []}
        }))
        .unwrap_err();
        assert!(error.message.contains("unknown field"));
    }

    #[test]
    fn jsonl_import_builds_and_verifies_a_branch() -> Result<(), Box<dyn Error>> {
        let (_dir, mut store) = test_store()?;
        let body = concat!(
            "{\"thread_id\":\"t\",\"checkpoint_id\":\"root\",\"checkpoint_no\":0,\"parent_checkpoint_id\":null,\"messages\":[{\"role\":\"user\",\"content\":\"help\"}]}\n",
            "{\"thread_id\":\"t\",\"checkpoint_id\":\"a\",\"checkpoint_no\":1,\"parent_checkpoint_id\":\"root\",\"messages\":[{\"role\":\"assistant\",\"content\":\"A\"}]}\n",
            "{\"thread_id\":\"t\",\"checkpoint_id\":\"b\",\"checkpoint_no\":1,\"parent_checkpoint_id\":\"root\",\"messages\":[{\"role\":\"assistant\",\"content\":\"B\"}]}\n"
        );
        let mut metrics = ServiceMetrics::new();
        let report = import_jsonl(
            &mut store,
            BufReader::new(Cursor::new(body.as_bytes())),
            &mut metrics,
        )?;
        assert_eq!(report.checkpoint_count, 3);
        assert_eq!(store.checkpoint_count(), 3);
        assert_eq!(workload_geometry(&store)["branch_point_count"], 1);
        assert_eq!(workload_geometry(&store)["leaf_count"], 2);
        assert_eq!(metrics.imported_checkpoints_total, 3);
        assert_eq!(store.verify_all()?.failures, 0);
        Ok(())
    }

    #[test]
    fn jsonl_import_reports_a_durable_partial_prefix() -> Result<(), Box<dyn Error>> {
        let (_dir, mut store) = test_store()?;
        let body = concat!(
            "{\"thread_id\":\"t\",\"checkpoint_id\":\"root\",\"checkpoint_no\":0,\"parent_checkpoint_id\":null,\"messages\":[\"root\"]}\n",
            "{\"thread_id\":\"t\",\"checkpoint_id\":\"bad\",\"checkpoint_no\":1,\"parent_checkpoint_id\":\"missing\",\"messages\":[\"bad\"]}\n"
        );
        let mut metrics = ServiceMetrics::new();
        let error = import_jsonl(
            &mut store,
            BufReader::new(Cursor::new(body.as_bytes())),
            &mut metrics,
        )
        .unwrap_err();
        assert!(error.contains("line 2 after 1 durable checkpoints"));
        assert_eq!(store.checkpoint_count(), 1);
        assert_eq!(store.verify_all()?.failures, 0);
        Ok(())
    }

    #[test]
    fn non_loopback_binding_requires_explicit_override() {
        assert!(is_loopback_bind("127.0.0.1:3210"));
        assert!(is_loopback_bind("[::1]:3210"));
        assert!(!is_loopback_bind("0.0.0.0:3210"));
    }

    #[test]
    fn dashboard_calls_live_api() {
        assert!(DASHBOARD_HTML.contains("/api/stats"));
        assert!(DASHBOARD_HTML.contains("/api/checkpoints?limit=100"));
        assert!(DASHBOARD_HTML.contains("alpha local evaluator"));
    }

    #[test]
    fn prometheus_surface_includes_store_workload_and_service_facts() -> Result<(), Box<dyn Error>>
    {
        let (_dir, store) = test_store()?;
        let metrics = ServiceMetrics::new();
        let output = prometheus_metrics(&store, &metrics).map_err(|error| error.message)?;
        assert!(output.contains("tulya_allocated_bytes"));
        assert!(output.contains("tulya_branch_points"));
        assert!(output.contains("tulya_durable_appends_total"));
        assert!(output.contains("tulya_last_append_sync_data_nanoseconds"));
        Ok(())
    }
}
