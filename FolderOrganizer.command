#!/bin/zsh

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_DIR"

PYTHON_BIN="$REPO_DIR/.venv/bin/python3"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

while true; do
  clear
  echo "Index-Friendly Folder Manager V2"
  echo "repo: $REPO_DIR"
  echo
  echo "1) 전체 계획 미리보기"
  echo "2) 현재 계획 바로 반영"
  echo "3) 기존 PARA 트리 마이그레이션 미리보기"
  echo "4) 기존 PARA 트리 마이그레이션 반영"
  echo "5) 아카이브 후보 미리보기"
  echo "6) 아카이브 실행"
  echo "7) V2 서비스 켜기"
  echo "8) V2 서비스 상태 보기"
  echo "9) 서비스 끄기"
  echo "10) repo outputs 정리"
  echo "11) 폴더 이동 후 경로 재연결"
  echo "q) Quit"
  echo
  read "choice?Select: "

  case "$choice" in
    1)
      "$PYTHON_BIN" "$REPO_DIR/quick_organizer.py" preview
      ;;
    2)
      "$PYTHON_BIN" "$REPO_DIR/quick_organizer.py" organize-now
      ;;
    3)
      "$PYTHON_BIN" "$REPO_DIR/quick_organizer.py" migrate-preview
      ;;
    4)
      "$PYTHON_BIN" "$REPO_DIR/quick_organizer.py" migrate-apply
      ;;
    5)
      "$PYTHON_BIN" "$REPO_DIR/quick_organizer.py" archive-preview
      ;;
    6)
      "$PYTHON_BIN" "$REPO_DIR/quick_organizer.py" archive-now
      ;;
    7)
      "$PYTHON_BIN" "$REPO_DIR/quick_organizer.py" service-on
      ;;
    8)
      "$PYTHON_BIN" "$REPO_DIR/quick_organizer.py" service-status
      ;;
    9)
      "$PYTHON_BIN" "$REPO_DIR/quick_organizer.py" service-off
      ;;
    10)
      "$PYTHON_BIN" "$REPO_DIR/quick_organizer.py" repair-outputs
      ;;
    11)
      "$REPO_DIR/scripts/after_repo_move.sh"
      ;;
    q|Q)
      exit 0
      ;;
    *)
      echo "잘못된 선택입니다."
      ;;
  esac

  echo
  read "pause?Press Enter to continue..."
done
