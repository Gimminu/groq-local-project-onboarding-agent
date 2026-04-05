#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_PARENT_DIR="$(cd "$REPO_DIR/.." && pwd)"
DEFAULT_CONFIG="$HOME/folder-organizer-v2.yml"
LIVE_CONFIG_CANDIDATE="$REPO_PARENT_DIR/folder-organizer-v2.live.yml"
SAMPLE_CONFIG="$REPO_DIR/samples/index_organizer_v2.obsidian_documents.yml"
CONFIG_PATH=""
SKIP_INSTALL=false
SKIP_SERVICE=false

pick_default_config() {
  if [[ -f "$DEFAULT_CONFIG" ]]; then
    echo "$DEFAULT_CONFIG"
    return
  fi

  if [[ -f "$LIVE_CONFIG_CANDIDATE" ]]; then
    echo "$LIVE_CONFIG_CANDIDATE"
    return
  fi

  echo "$SAMPLE_CONFIG"
}

resolve_config_path() {
  local requested_path="$1"

  if [[ -f "$requested_path" ]]; then
    echo "$requested_path"
    return
  fi

  if [[ "$(basename "$requested_path")" == "folder-organizer-v2.yml" ]] && [[ -f "$LIVE_CONFIG_CANDIDATE" ]]; then
    echo "WARN: requested config not found: $requested_path" >&2
    echo "WARN: using fallback config: $LIVE_CONFIG_CANDIDATE" >&2
    echo "$LIVE_CONFIG_CANDIDATE"
    return
  fi

  echo "$requested_path"
}

print_help() {
  cat <<'EOF'
Repair runtime after moving this repository

Usage:
  ./scripts/after_repo_move.sh [--config <path>] [--skip-install] [--skip-service]

Options:
  --config <path>   Organizer config path (default order:
                    ~/folder-organizer-v2.yml -> ../folder-organizer-v2.live.yml -> samples/...)
  --skip-install    Skip editable pip reinstall
  --skip-service    Skip launchd service reinstall

What this does:
1) (optional) Reinstall package in editable mode from new repo path
2) (optional) Reinstall launchd service so plist points to new repo path
3) Verify service status and run organizer status
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --config requires a value"
        exit 1
      fi
      CONFIG_PATH="$2"
      shift 2
      ;;
    --skip-install)
      SKIP_INSTALL=true
      shift
      ;;
    --skip-service)
      SKIP_SERVICE=true
      shift
      ;;
    -h|--help)
      print_help
      exit 0
      ;;
    *)
      echo "ERROR: unknown option: $1"
      print_help
      exit 1
      ;;
  esac
done

if [[ -z "$CONFIG_PATH" ]]; then
  CONFIG_PATH="$(pick_default_config)"
else
  CONFIG_PATH="$(resolve_config_path "$CONFIG_PATH")"
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "ERROR: config not found: $CONFIG_PATH"
  echo "hint=Try one of:" 
  echo "  --config $LIVE_CONFIG_CANDIDATE"
  echo "  --config $SAMPLE_CONFIG"
  exit 1
fi

PYTHON_BIN="$REPO_DIR/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

if [[ -z "$PYTHON_BIN" ]]; then
  echo "ERROR: python3 not found"
  exit 1
fi

echo "repo=$REPO_DIR"
echo "python=$PYTHON_BIN"
echo "config=$CONFIG_PATH"

if [[ "$SKIP_INSTALL" == "false" ]]; then
  echo "[1/3] reinstall editable package"
  "$PYTHON_BIN" -m pip install -e "$REPO_DIR"
else
  echo "[1/3] skipped editable reinstall"
fi

if [[ "$SKIP_SERVICE" == "false" ]]; then
  echo "[2/3] reinstall launchd service"
  "$PYTHON_BIN" "$REPO_DIR/organizer.py" service-install --config "$CONFIG_PATH"
else
  echo "[2/3] skipped service reinstall"
fi

PLIST_PATH="$HOME/Library/LaunchAgents/com.groqmcp.index-organizer.v2.service.plist"
if [[ -f "$PLIST_PATH" ]]; then
  if /usr/bin/plutil -p "$PLIST_PATH" | grep -Fq "$REPO_DIR/organizer.py"; then
    echo "plist_path_check=ok"
  else
    echo "plist_path_check=warning"
    echo "hint=LaunchAgent may still point to old path. Run service-install again."
  fi
else
  echo "plist_path_check=missing"
fi

echo "[3/3] runtime status"
"$PYTHON_BIN" "$REPO_DIR/organizer.py" service-status
"$PYTHON_BIN" "$REPO_DIR/index_organizer.py" status --config "$CONFIG_PATH" --review-limit 5

echo "done=after_repo_move"
