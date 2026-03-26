# Automation Checks

GitHub Actions 없이 로컬/서버에서 자동 점검을 실행하기 위한 모듈입니다.

## 실행 모드

- `non-s3`: S3를 제외한 로컬 점검만 실행
- `s3-only`: S3 무결성 점검만 실행
- `all`: 전체 점검 실행

## 실행 예시

```bash
python3 -m automation.run_checks --mode non-s3 --config automation/config.json
python3 -m automation.run_checks --mode s3-only --config automation/config.json
python3 -m automation.run_checks --mode all --config automation/config.json
```

`--fail-on-warn` 옵션을 주면 WARN도 실패(exit 1)로 처리합니다.

## S3 IO 최소화

`automation/config.json`의 `checks.s3_integrity`에서 아래 값을 줄이면 S3 호출량을 줄일 수 있습니다.

- `max_logs`: 확인할 최근 로그 파일 수
- `sample_size`: `head_object` 샘플 개수

