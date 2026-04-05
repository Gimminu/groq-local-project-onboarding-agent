#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-}"
TARGET_CONFIG="${2:-$HOME/folder-organizer-v2.yml}"
NO_RELOAD="${3:-}"

print_help() {
  cat <<'EOF'
Set organizer runtime profile

Usage:
  ./scripts/set_power_profile.sh power-saver [target-config]
  ./scripts/set_power_profile.sh standard [target-config]
  ./scripts/set_power_profile.sh power-saver [target-config] --no-reload

Examples:
  ./scripts/set_power_profile.sh power-saver ~/folder-organizer-v2.yml
  ./scripts/set_power_profile.sh standard ~/folder-organizer-v2.yml

Notes:
- target-config is overwritten (a timestamp backup is created when the file exists).
- On macOS, this script also refreshes launchd service with the selected config.
- Add --no-reload to skip service reload.
EOF
}

case "$PROFILE" in
  power-saver)
    SOURCE_CONFIG="$REPO_DIR/samples/index_organizer_v2.obsidian_power_saver.yml"
    ;;
  standard)
    SOURCE_CONFIG="$REPO_DIR/samples/index_organizer_v2.obsidian_documents.yml"
    ;;
  help|-h|--help|"")
    print_help
    exit 0
    ;;
  *)
    echo "ERROR: unknown profile '$PROFILE'"
    print_help
    exit 1
    ;;
esac

if [[ ! -f "$SOURCE_CONFIG" ]]; then
  echo "ERROR: source profile not found: $SOURCE_CONFIG"
  exit 1
fi

mkdir -p "$(dirname "$TARGET_CONFIG")"

if [[ -f "$TARGET_CONFIG" ]]; then
  backup_path="${TARGET_CONFIG}.bak.$(date +"%Y%m%d_%H%M%S")"
  cp "$TARGET_CONFIG" "$backup_path"
  echo "Backup created: $backup_path"
fi

cp "$SOURCE_CONFIG" "$TARGET_CONFIG"
echo "Applied profile '$PROFILE' to: $TARGET_CONFIG"

if [[ "$NO_RELOAD" == "--no-reload" ]]; then
  echo "Skipped service reload (--no-reload)."
  exit 0
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  PYTHON_BIN="$REPO_DIR/.venv/bin/python"
  if [[ ! -x "$PYTHON_BIN" ]]; then
    PYTHON_BIN="$(command -v python3)"
  fi

  "$PYTHON_BIN" "$REPO_DIR/organizer.py" service-install --config "$TARGET_CONFIG"
  "$PYTHON_BIN" "$REPO_DIR/organizer.py" service-status
else
  echo "Non-macOS detected: skipped launchd service refresh."
fi
