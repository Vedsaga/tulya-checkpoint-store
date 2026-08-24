# Security policy

Tulya 0.1 is pre-production single-writer storage. It has process-crash tests,
but no sudden-power-loss, encryption-at-rest, untrusted-input sandbox,
multi-writer, or distributed-consensus claim. The optional `tulya-local`
evaluator exposes unauthenticated HTTP on loopback by default. It is not a
production network service and must not be exposed to an untrusted network.

Please avoid posting an exploitable report publicly. Use GitHub's private
security-advisory flow for the repository, including the affected version,
filesystem/OS, reproduction, impact, and whether data integrity or
confidentiality is involved.
