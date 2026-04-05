# outputs

repo 루트 `outputs/`는 현재 두 가지 용도로만 남겨둡니다.

- `onboarding-agent/reports/`: `main.py`가 쓰는 온보딩 에이전트 결과
- `legacy-v1/`: 예전 organizer V1 보고서, launchd 로그, runtime 파일 보관

현재 V2 organizer의 실제 보고서와 서비스 상태는 repo 밖의 아래 경로를 사용합니다.

- `~/Library/Application Support/FolderOrganizerV2/reports/`
- `~/Library/Application Support/FolderOrganizerV2/service-state.json`

루트에 파일이 다시 쌓였으면 아래 명령으로 정리할 수 있습니다.

```bash
python3 organizer.py repair-outputs --config ~/folder-organizer-v2.yml --apply
```
