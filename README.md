# distbuild

Educational distributed job runner (API server + workers) with per-consumer API keys, quotas, sandboxed execution, and per-job logs.

This repository was created by a human using OpenCode (https://opencode.ai/) with the free model GPT-5.2 from OpenAI.
No code in this repo was written directly by a human; it was authored via OpenCode + the model.

DO NOT deploy this to production.

- Expect bugs and missing hardening.
- Do not run untrusted code unless you understand the isolation boundaries.
- Use it for learning, demos, and experimentation.

## Features

- API server accepts job submissions.
- Per-consumer API keys (hashed at rest) + enable/disable.
- Per-consumer quotas: max concurrent jobs + max jobs/day.
- Workers claim jobs and execute in a sandbox:
  - `docker` sandbox (preferred): per-job isolated network (with internet by default), dropped capabilities, no-new-privileges, resource limits; runs as root by default.
  - `local` sandbox (dev only): best-effort rlimits; not strong isolation.
- Job logs stored and retrievable by the submitting consumer.
- Client CLI (`distbuild`) with config profiles.

## Quick Start (Local)

1) Install

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
```

2) Start API + worker

```bash
export DISTBUILD_WORKER_SHARED_TOKEN='change-me'

distbuild-api --host 0.0.0.0 --port 8080 --worker-token "$DISTBUILD_WORKER_SHARED_TOKEN"
```

In another terminal:

```bash
. .venv/bin/activate
export DISTBUILD_WORKER_SHARED_TOKEN='change-me'
distbuild-worker --server http://127.0.0.1:8080 --worker-token "$DISTBUILD_WORKER_SHARED_TOKEN" --worker-id worker-1
```

3) Create a consumer ("person")

```bash
distbuild-admin create-consumer --name alice --max-concurrent 2 --max-per-day 50
```

Copy the printed `consumer_key`.

4) Configure the client CLI

```bash
distbuild init --server http://127.0.0.1:8080 --consumer-key "<kid>.<db_...>" --profile dev
distbuild use dev
distbuild ping
```

5) Submit a job

```bash
distbuild submit --command "echo hello && uname -a" --follow
```

## Daemon Mode

Run API + worker in the background (writes pid/log under `XDG_RUNTIME_DIR` when set; otherwise under `~/.cache/distbuild/run`).

```bash
export DISTBUILD_WORKER_SHARED_TOKEN='change-me'

distbuild-api --start --port 8080 --worker-token "$DISTBUILD_WORKER_SHARED_TOKEN"
distbuild-worker --start --server http://127.0.0.1:8080 --worker-token "$DISTBUILD_WORKER_SHARED_TOKEN" --worker-id worker-1

distbuild-api --status --port 8080
distbuild-worker --status --worker-id worker-1

distbuild-worker --stop --worker-id worker-1
distbuild-api --stop --port 8080
```

## Config Files (Optional)

API config (`api.toml`):

```toml
host = "0.0.0.0"
port = 8080
db_url = "sqlite:////absolute/path/to/distbuild.db"
worker_token = "change-me"
allow_local_sandbox = true
log_level = "info"
access_log = true
```

```bash
distbuild-api --config-file api.toml
distbuild-api --start --config-file api.toml
```

Worker config (`worker.toml`):

```toml
server = "http://127.0.0.1:8080"
worker_token = "change-me"
worker_id = "worker-1"
poll_interval = 0.5
log_level = "info"
```

```bash
distbuild-worker --config-file worker.toml
distbuild-worker --start --config-file worker.toml
```

Docker sandbox settings (env vars, evaluated by workers):

```bash
# Default job image (if the submitted job doesn't specify one)
export DISTBUILD_DOCKER_DEFAULT_IMAGE='debian:bookworm-slim'

# Networking: job|bridge|none|<existing-network-name>
export DISTBUILD_DOCKER_NETWORK_MODE='job'

# Container user: root|nobody|UID:GID
export DISTBUILD_DOCKER_RUN_AS='root'

# Capabilities to add back after dropping ALL (comma-separated).
# Default is "build-friendly" so apt/ping work.
export DISTBUILD_DOCKER_CAP_ADD='CHOWN,DAC_OVERRIDE,FOWNER,SETUID,SETGID,NET_RAW'

# Optional hardening (may break package installs)
export DISTBUILD_DOCKER_READ_ONLY_ROOTFS='false'
```

## distbuild CLI

Common commands:

```bash
distbuild profiles
distbuild config
distbuild list --limit 20

distbuild submit --command "echo hello" --wait
distbuild submit --command-file build.sh --follow
cat build.sh | distbuild submit --stdin --follow

distbuild status <job_id>
distbuild logs <job_id>
distbuild tail <job_id>
```

Profiles let you keep multiple server/key pairs:

```bash
distbuild init --server http://127.0.0.1:8080 --consumer-key "<kid>.<db_...>" --profile dev
distbuild init --server https://build.example.com --consumer-key "<kid>.<db_...>" --profile prod --no-set-active
distbuild profiles
distbuild use prod
```

## Admin CLI

```bash
distbuild-admin list-consumers
distbuild-admin disable-consumer --name alice
distbuild-admin enable-consumer --name alice
distbuild-admin set-quota --name alice --max-concurrent 4 --max-per-day 200
distbuild-admin rotate-key --name alice
distbuild-admin delete-consumer --name alice --yes
distbuild-admin reset-db --yes
distbuild-admin db-info
```

## API

- `POST /v1/jobs` submit a job (`X-Consumer-Key` header required)
- `GET /v1/jobs` list jobs for the consumer
- `GET /v1/jobs/{job_id}` job status
- `GET /v1/jobs/{job_id}/logs` paged logs

## Security Notes (Read This)

- The `local` sandbox is not a security boundary.
- The `docker` sandbox is better, but containers are not a VM boundary.
- For hostile code, run workers on dedicated machines and consider stronger isolation (gVisor/Kata/Firecracker).

## License

MIT (see `LICENSE`).
