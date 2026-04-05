#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_NAME="${IMAGE_NAME:-folder-organizer:v2}"
SANDBOX_DIR="$REPO_DIR/.docker-sandbox"
CONFIG_FILE="$SANDBOX_DIR/config.yml"
DATA_DIR="$SANDBOX_DIR/data"
STATE_DIR="$SANDBOX_DIR/state"

print_help() {
  cat <<'EOF'
Easy Docker helper for Folder Organizer

Usage:
  bash scripts/docker_easy.sh init
  bash scripts/docker_easy.sh build
  bash scripts/docker_easy.sh status
  bash scripts/docker_easy.sh plan
  bash scripts/docker_easy.sh tick
  bash scripts/docker_easy.sh apply
  bash scripts/docker_easy.sh where

Notes:
- init  : create a safe local sandbox config and folders.
- build : build Docker image (default: folder-organizer:v2).
- status/plan/tick/apply run inside container with sandbox mounts.
- This helper does NOT touch ~/Documents unless you edit config.yml yourself.
EOF
}

ensure_docker_daemon() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker command not found. Install Docker Desktop first."
    exit 1
  fi
  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker daemon is not running. Start Docker Desktop and retry."
    exit 1
  fi
}

write_default_config() {
  cat >"$CONFIG_FILE" <<'YAML'
spaces_root: /data
state_dir: /state

watch_roots:
  - /data

root_spaces:
  "/data": main

spaces:
  - main

streams:
  - inbox
  - projects
  - areas
  - resources
  - archive
  - review
  - system

domains:
  - unknown
  - coding
  - research
  - admin
  - finance

groq:
  enabled: false

llm:
  preferred_provider: ollama
  fallback_to_other_cloud: false
  fallback_to_ollama: false
  enable_for_watch: false
  enable_llm_first: false

watch:
  poll_interval_seconds: 15
  stable_observation_seconds: 30
  staging_age_seconds: 120

service:
  tick_interval_seconds: 60
  maintenance_interval_seconds: 3600
  archive_interval_seconds: 86400
  startup_apply: false
  startup_archive: false
YAML
}

init_sandbox() {
  mkdir -p "$DATA_DIR/inbox" "$STATE_DIR"

  migrated_config=false
  if [[ ! -f "$CONFIG_FILE" ]]; then
    write_default_config
  elif grep -q '"/data/inbox": main' "$CONFIG_FILE"; then
    cp "$CONFIG_FILE" "$CONFIG_FILE.bak"
    write_default_config
    migrated_config=true
  fi

  if [[ ! -f "$DATA_DIR/inbox/README_DROP_FILES_HERE.txt" ]]; then
    cat >"$DATA_DIR/inbox/README_DROP_FILES_HERE.txt" <<'TXT'
Drop test files into this folder and run:
  bash scripts/docker_easy.sh plan
  bash scripts/docker_easy.sh apply
TXT
  fi

  cat <<EOF
Sandbox ready.
- config: $CONFIG_FILE
- inbox : $DATA_DIR/inbox
- state : $STATE_DIR
EOF

  if [[ "$migrated_config" == "true" ]]; then
    echo "Migrated legacy sandbox config. Backup: $CONFIG_FILE.bak"
  fi
}

ensure_sandbox() {
  if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Sandbox not initialized yet. Running init..."
    init_sandbox
  fi
}

build_image() {
  ensure_docker_daemon
  docker build -t "$IMAGE_NAME" "$REPO_DIR"
}

run_in_container() {
  ensure_docker_daemon
  ensure_sandbox
  docker run --rm -it \
    -v "$CONFIG_FILE:/config/folder-organizer-v2.yml:ro" \
    -v "$DATA_DIR:/data" \
    -v "$STATE_DIR:/state" \
    "$IMAGE_NAME" "$@"
}

command_name="${1:-help}"

case "$command_name" in
  init)
    init_sandbox
    ;;
  build)
    build_image
    ;;
  status)
    run_in_container status --config /config/folder-organizer-v2.yml
    ;;
  plan)
    run_in_container plan --config /config/folder-organizer-v2.yml
    ;;
  tick)
    run_in_container service-tick --config /config/folder-organizer-v2.yml
    ;;
  apply)
    run_in_container apply --config /config/folder-organizer-v2.yml --apply
    ;;
  where)
    ensure_sandbox
    cat <<EOF
Sandbox paths:
- config: $CONFIG_FILE
- inbox : $DATA_DIR/inbox
- state : $STATE_DIR
EOF
    ;;
  help|-h|--help)
    print_help
    ;;
  *)
    echo "Unknown command: $command_name"
    print_help
    exit 1
    ;;
esac
