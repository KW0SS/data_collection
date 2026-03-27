# collect_test

## 트리거
- "데이터 수집 혹은 테스트 해줘"
- "데이터 수집해줘", "수집 테스트 해줘", "단일 테스트 해줘", "파이프라인 테스트"

## 목표
수집/테스트 요청을 받으면 3단계(리스트 확인 → 단일 테스트 → 본 수집) 기준으로
즉시 실행 가능한 명령어를 추천하고, 요청 시 실제 테스트 실행까지 진행한다.

## 입력 규칙
- `status` 기본값: `normal`
- `sectors` 기본값: `"Materials"`
- `member` 기본값: 사용자 지정값, 없으면 실행 전 확인 필요
- 2015년 이전 테스트는 `collect.py collect --years <2015`로 legacy 자동 분기

## 추천 출력 포맷 (항상 유지)
1. 리스트 확인 (로컬 X / S3 X)
2. 단일 테스트 (로컬 O / S3 O)
3. 본 수집 실행 (로컬 O / S3 O)

## 실행 명령 예시
```bash
# 1) 리스트 확인 (수집 실행 X)
python3 run_pipeline.py --status <status> --sectors "<sector>" --member <member> --dry-run

# 2) 단일 테스트 (단일 종목)
python3 collect.py collect --stock-codes <stock_code> --years <year> --quarters ANNUAL --save-raw --upload-s3

# 3) 본 수집 실행
python3 run_pipeline.py --status <status> --sectors "<sector>" --member <member>
```

## 결과 형식
1. 단계별 명령어 3개
2. 단계별 로컬/S3 여부
3. 주의사항 1~2개 (예: `.env` S3 키, `member` 필수)
