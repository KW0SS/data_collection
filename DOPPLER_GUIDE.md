# Doppler 환경변수 관리 가이드

이 프로젝트는 `.env` 파일 또는 **Doppler Service Token**을 통해 환경변수(시크릿)를 관리할 수 있습니다.

---

## 관리 대상 환경변수

| Key | 설명 | 필수 |
|---|---|---|
| `DART_API_KEY` | OpenDART API 키 | ✅ |
| `S3_ACCESS_KEY` | AWS Access Key ID | S3 사용 시 |
| `S3_PRIVATE_KEY` | AWS Secret Access Key | S3 사용 시 |
| `S3_BUCKET_NAME` | S3 버킷 이름 | S3 사용 시 |
| `S3_REGION` | AWS 리전 (기본: `ap-northeast-2`) | 선택 |

---

## 1. Service Token 발급

1. [Doppler 대시보드](https://dashboard.doppler.com) 접속
2. **프로젝트** → **환경 선택** (예: `prod`)
3. **Access** 탭 → **+ Generate Service Token**
4. 토큰 복사: `dp.st.prod.aBcDeFgH1234...`

> ⚠️ Service Token은 **특정 프로젝트 + 특정 환경**에만 접근 가능한 읽기 전용 토큰입니다.

---

## 2. 사용 방법

서버에 `DOPPLER_TOKEN` 하나만 등록합니다:

```bash
export DOPPLER_TOKEN="dp.st.prod.aBcDeFgH1234..."
```

이후 REST API 한 줄로 `.env` 파일을 생성합니다:

```bash
curl -sf \
  --url "https://api.doppler.com/v3/configs/config/secrets/download?format=env" \
  --header "authorization: Bearer $DOPPLER_TOKEN" \
  > .env
```

코드 실행:

```bash
python3 collect.py collect --companies data/input/companies.csv --years 2023 --upload-s3
```

---

## 3. 환경변수 주입 흐름

```
┌──────────────────────────┐
│     Doppler Cloud        │  ← 시크릿 저장소
│  DART_API_KEY = abc123   │
│  S3_ACCESS_KEY = AKIA... │
│  S3_PRIVATE_KEY = wJal.. │
│  S3_BUCKET_NAME = kw0ss. │
└────────────┬─────────────┘
             │ HTTPS (Bearer Token 인증)
             ▼
┌──────────────────────────┐
│  curl 호출               │  ← Service Token으로 API 요청
│  → .env 파일 생성         │  ← 응답을 파일로 저장
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│  python3 collect.py      │  ← 코드가 .env 읽어서 사용
│  os.getenv() → .env 폴백  │
└──────────────────────────┘
```

---

## 4. 배포 스크립트 예시

```bash
#!/bin/bash
# deploy.sh
set -e

# Doppler에서 최신 시크릿 다운로드
curl -sf \
  --url "https://api.doppler.com/v3/configs/config/secrets/download?format=env" \
  --header "authorization: Bearer $DOPPLER_TOKEN" \
  > .env

# 데이터 수집 실행
python3 collect.py collect \
  --companies data/input/companies.csv \
  --years 2023 \
  --upload-s3
```

---

## 참고

- `.env` 파일과 Doppler는 공존 가능합니다. Doppler가 없는 환경에서도 `.env` 파일만 있으면 정상 동작합니다.
- 시크릿 변경 시 Doppler 대시보드에서 수정하면, 다음 `curl` 호출 시 자동 반영됩니다.
- 서버에는 Doppler CLI 설치가 필요 없습니다. `curl`만 있으면 됩니다.
