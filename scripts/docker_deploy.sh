#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_IMAGE="${LOCAL_IMAGE:-folder-organizer:v2}"
DIST_DIR="$REPO_DIR/dist"

print_help() {
  cat <<'EOF'
Docker deploy helper (build/package/push)

Usage:
  ./scripts/docker_deploy.sh build
  ./scripts/docker_deploy.sh save [output.tar]
  ./scripts/docker_deploy.sh push <registry/image:tag>
  ./scripts/docker_deploy.sh all [registry/image:tag]

Environment:
  LOCAL_IMAGE   Local build image tag (default: folder-organizer:v2)

Notes:
- This script is for deployment packaging only.
- Keep day-to-day runtime on native macOS launchd service.
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

build_image() {
  ensure_docker_daemon
  echo "Building image: $LOCAL_IMAGE"
  docker build -t "$LOCAL_IMAGE" "$REPO_DIR"
}

save_image() {
  ensure_docker_daemon
  mkdir -p "$DIST_DIR"

  local output_path="${1:-}"
  if [[ -z "$output_path" ]]; then
    local stamp
    stamp="$(date +"%Y%m%d_%H%M%S")"
    output_path="$DIST_DIR/${LOCAL_IMAGE//[:\/]/_}_${stamp}.tar"
  fi

  echo "Saving image to: $output_path"
  docker save "$LOCAL_IMAGE" -o "$output_path"
  echo "Saved: $output_path"
}

push_image() {
  ensure_docker_daemon
  local target_image="${1:-}"
  if [[ -z "$target_image" ]]; then
    echo "ERROR: target image is required."
    echo "Example: ./scripts/docker_deploy.sh push ghcr.io/<owner>/folder-organizer:v2"
    exit 1
  fi

  echo "Tagging: $LOCAL_IMAGE -> $target_image"
  docker tag "$LOCAL_IMAGE" "$target_image"
  echo "Pushing: $target_image"
  docker push "$target_image"
}

command_name="${1:-help}"

case "$command_name" in
  build)
    build_image
    ;;
  save)
    save_image "${2:-}"
    ;;
  push)
    push_image "${2:-}"
    ;;
  all)
    build_image
    save_image
    if [[ -n "${2:-}" ]]; then
      push_image "$2"
    fi
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
