# 상폐 예측 모델용 재무비율 데이터 수집 도구

코스닥 정상/상폐 기업의 **분기별 재무비율 30개**를 OpenDART API로 수집하여 CSV로 저장하고, 원본 재무제표 JSON을 S3에 업로드하는 도구입니다.

---

## 빠른 시작

```bash
# 1. 의존성 설치
pip3 install -r requirements.txt

# 2. .env 파일 생성 (API 키 설정)
cp .env.example .env   # 또는 직접 생성

# 3. 엑셀 파일 배치 (아래 '사전 준비' 참고)

# 4. 통합 파이프라인 실행
python3 run_pipeline.py --status normal --sectors "Materials" --member hann --skip-s3
```

---

## 프로젝트 구조

```
data_collection/
├── run_pipeline.py                    # 통합 파이프라인 (기업 목록 → 수집 → S3)
├── collect.py                         # CLI 진입점 (개별 수집)
├── src/
│   ├── dart_api.py                    # OpenDART API 클라이언트
│   ├── account_mapper.py              # DART 계정과목명 → 표준 키 매핑
│   ├── ratio_calculator.py            # 30개 재무비율 계산기
│   ├── collector.py                   # 데이터 수집 오케스트레이터
│   └── s3_uploader.py                 # S3 업로드 모듈 (GICS 섹터별)
├── f_make_A_input.py                  # (보조) 정상 기업 목록 생성 스크립트
├── f_make_delisted_input.py           # (보조) 상폐 기업 목록 생성 스크립트
├── f_fetch_induty_codes.py            # 상폐 기업 업종코드 조회 (DART API)
├── f_split_companies.py               # 기업 목록 분할 도구
├── data/
│   ├── input/                         # 기업 목록 CSV + KRX 엑셀
│   │   └── krx_all_companies.xlsx     # (수동 배치) KRX 전종목 엑셀
│   ├── etc/                           # (수동 배치) 상폐 현황 + 기업코드
│   │   ├── 상장폐지현황.xlsx            # KRX 상장폐지현황
│   │   └── corp_codes.xml             # DART 고유번호 전체
│   ├── output/                        # 결과 재무비율 CSV (GICS 섹터별)
│   └── raw/                           # 원본 재무제표 JSON (선택)
├── requirements.txt
└── .env                               # API 키 및 S3 설정
```

---

## 사전 준비

### 1. 환경 설정

- **Python** 3.10 이상
- **OpenDART API 키** ([https://opendart.fss.or.kr](https://opendart.fss.or.kr) 에서 발급)

```bash
pip3 install -r requirements.txt
```

### 2. `.env` 파일 설정

프로젝트 루트에 `.env` 파일을 생성합니다:

```
DART_API_KEY=발급받은_API_키

# S3 업로드 기능 사용 시
S3_ACCESS_KEY=AWS_Access_Key_ID
S3_PRIVATE_KEY=AWS_Secret_Access_Key
S3_BUCKET_NAME=my-bucket-name
S3_REGION=ap-northeast-2
```

### 3. 엑셀 파일 배치

수집 대상에 따라 필요한 파일이 다릅니다:

| 파일 | 경로 | 용도 | 필요 시점 |
|---|---|---|---|
| KRX 전종목 엑셀 | `data/input/krx_all_companies.xlsx` | 정상 기업 목록 생성 | `--status normal` 또는 `all` |
| 상장폐지현황 엑셀 | `data/etc/상장폐지현황.xlsx` | 상폐 기업 목록 생성 | `--status delisted` 또는 `all` |
| DART 고유번호 XML | `data/etc/corp_codes.xml` | 상폐 기업 업종코드 조회 | `--status delisted` (업종코드 캐시 없을 때) |

**다운로드 방법:**
- KRX 전종목: [KRX 정보데이터시스템](http://data.krx.co.kr) → 기본통계 → 전종목 기본정보 → 엑셀 다운로드
- 상장폐지현황: [KRX KIND](https://kind.krx.co.kr) → 상장폐지종목 → 엑셀 다운로드
- DART 고유번호: [DART OpenAPI](https://opendart.fss.or.kr) → 기업코드 다운로드 (ZIP 해제 후 XML)

---

## 사용법

### 방법 1: 통합 파이프라인 (`run_pipeline.py`) — 권장

엑셀 파일만 배치하면 **기업 목록 생성 → 재무제표 수집 → S3 업로드**를 한 번에 수행합니다.

```bash
# 정상 기업만, Materials 섹터
python3 run_pipeline.py --status normal \
    --sectors "Materials" \
    --member hann

# 상폐 기업만, Health Care 섹터
python3 run_pipeline.py --status delisted \
    --sectors "Health Care" \
    --member hann

# 정상 + 상폐 모두, 복수 섹터
python3 run_pipeline.py --status all \
    --sectors "Information Technology" "Consumer Discretionary" "Materials" \
    --member hann

# 전체 섹터 (--sectors 미지정)
python3 run_pipeline.py --status all --member hann

# S3 업로드 건너뛰기 (수집만)
python3 run_pipeline.py --status normal \
    --sectors "Materials" \
    --member hann --skip-s3

# dry-run: 수집 대상 기업 목록만 확인
python3 run_pipeline.py --status delisted \
    --sectors "Materials" \
    --member hann --dry-run

# 이미 수집된 데이터도 재수집
python3 run_pipeline.py --status normal \
    --sectors "Materials" \
    --member hann --force

# 2015년 이전 데이터 포함 (dart-fss 자동 사용)
python3 run_pipeline.py --status delisted \
    --sectors "Materials" \
    --member hann --start-year 2002 --skip-s3

# 정상 기업 2010~2025 전체 수집
python3 run_pipeline.py --status normal \
    --sectors "Information Technology" \
    --member hann --start-year 2010 --end-year 2025
```

> **2015년 이전 데이터:** DART OpenAPI(`fnlttSinglAcntAll`)는 2015년 이후만 지원합니다.
> `--start-year`를 2015 미만으로 지정하면 해당 구간은 `dart-fss` 라이브러리를 통해
> XBRL/HTML 원문에서 사업보고서(ANNUAL) 단위로 자동 수집됩니다.

#### `run_pipeline.py` CLI 옵션

| 옵션 | 필수 | 설명 |
|---|---|---|
| `--status` | O | 수집 대상: `normal`(정상), `delisted`(상폐), `all`(전체) |
| `--sectors` | X | GICS 섹터 목록 (미지정 시 전체 섹터). 대소문자/별칭 허용 (`materials`, `health care` 등) |
| `--member` | O | 작업자 이름 (S3 로그 기록용) |
| `--start-year` | X | 수집 시작 연도 (기본: 2015). 2015 미만이면 dart-fss로 legacy 수집 |
| `--end-year` | X | 수집 종료 연도 (기본: 2025) |
| `--skip-s3` | X | S3 업로드 건너뛰기 |
| `--force` | X | 이미 수집된 데이터도 재수집 |
| `--dry-run` | X | 수집 대상 기업 목록만 출력하고 종료 |

#### 지원 GICS 섹터

- `Information Technology`
- `Communication Services`
- `Consumer Discretionary`
- `Consumer Staples`
- `Health Care`
- `Industrials`
- `Materials`
- `Energy`
- `Utilities`
- `Financials`
- `Real Estate`

#### 파이프라인 동작 흐름

```
엑셀 파일 (수동 배치)
      │
      ▼
[1/3] 기업 목록 생성
      ├── 정상 기업: KRX 엑셀 → 업종 기반 범용 GICS 매핑
      └── 상폐 기업: 업종코드 캐시/조회 → GICS 매핑 → 재무적 리스크 필터링
      │
      ▼
[2/3] 재무제표 수집
      ├── 2015+: DART OpenAPI로 분기별 재무제표 조회
      ├── 2015 미만: dart-fss로 XBRL/HTML ANNUAL 단위 수집
      ├── 계정과목명 표준화 → 30개 재무비율 계산
      └── CSV 저장: data/output/{섹터}/{종목코드}_{연도}.csv
      │
      ▼
[3/3] S3 업로드
      └── 원본 JSON을 s3://{bucket}/{healthy|delisted}/{섹터}/... 로 업로드
```

---

### 방법 2: 개별 수집 (`collect.py`)

기업 목록이 이미 있거나 개별 종목을 직접 수집할 때 사용합니다.

#### 종목코드로 직접 수집

```bash
# 단일 기업, 특정 연도
python3 collect.py collect --stock-codes 019440 --years 2023

# 복수 기업, 복수 연도
python3 collect.py collect --stock-codes 019440 005930 --years 2020 2021 2022 2023

# 특정 분기만 지정
python3 collect.py collect --stock-codes 019440 --years 2023 --quarters Q1 ANNUAL

# 원본 재무제표 JSON도 함께 저장
python3 collect.py collect --stock-codes 019440 --years 2023 --save-raw

# 저장 디렉터리 지정
python3 collect.py collect --stock-codes 019440 --years 2023 -o data/output/my_folder/
```

#### 기업 목록 CSV로 배치 수집

```csv
stock_code,corp_name,label,gics_sector,start_year,end_year
019440,세아특수강,1,Materials,2020,2023
005930,삼성전자,0,Information Technology,2021,2024
035720,카카오,0,Communication Services,,
```

| 컬럼 | 필수 | 설명 |
|---|---|---|
| `stock_code` | O | 종목코드 (6자리) |
| `corp_name` | O | 기업명 |
| `label` | O | 0=정상, 1=상폐 |
| `gics_sector` | X | GICS 섹터명 (S3 업로드/디렉터리 구분용) |
| `start_year` | X | 수집 시작 연도 (미지정 시 `--years` 사용) |
| `end_year` | X | 수집 종료 연도 |

```bash
python3 collect.py collect --companies data/input/companies.csv
```

#### 기업 검색

```bash
python3 collect.py search --name 세아특수강
python3 collect.py search --stock-code 019440
python3 collect.py search --name 삼성 --limit 10
```

#### 누락 데이터 재수집 (`collect.py retry`)

파이프라인 실행 후 누락된 데이터를 별도로 확인하고 재수집합니다.
`companies_collected.csv`와 `data/output/` 디렉터리를 비교하여 누락을 자동 탐지합니다.

```bash
# 누락 목록만 확인 (수집하지 않음)
python3 collect.py retry --check-only

# 특정 종목만 재수집
python3 collect.py retry --stock-codes 052670 048260 --save-raw

# 전체 누락 데이터 재수집
python3 collect.py retry --save-raw

# S3 업로드 포함
python3 collect.py retry --stock-codes 048260 --save-raw --upload-s3
```

| 옵션 | 설명 |
|---|---|
| `--stock-codes` | 재수집할 종목코드 목록 (미지정 시 전체 누락 대상) |
| `--check-only` | 누락 목록(`companies_missing.csv`)만 생성하고 종료 |
| `--fs-div` | CFS/OFS (기본: CFS) |
| `--output-dir` | output 디렉터리 경로 |
| `--save-raw` | 원본 JSON 저장 |
| `--upload-s3` | S3 업로드 |
| `--delay` | API 호출 간 대기 초 (기본: 0.5) |

> 누락 목록은 `data/input/companies_missing.csv`에 저장되며, legacy(2015 이전)와 modern(2015 이후) 구간이 자동 분류됩니다.

#### `collect.py` CLI 옵션

```
collect.py collect
  --stock-codes       종목코드 목록 (예: 019440 005930)
  --companies         기업 목록 CSV 파일 경로
  --years             수집 연도 (기본: 2023)
  --quarters          수집 분기 (Q1, H1, Q3, ANNUAL / 기본: 전체)
  --fs-div            CFS=연결재무제표, OFS=별도재무제표 (기본: CFS)
  --output-dir, -o    결과 CSV 저장 디렉터리 (기본: data/output/)
  --save-raw          원본 재무제표 JSON을 data/raw/에 저장
  --upload-s3         원본 재무제표 JSON을 S3에 업로드
  --s3-bucket         S3 버킷 이름 (없으면 .env의 S3_BUCKET_NAME)
  --s3-region         AWS 리전 (없으면 .env의 S3_REGION)
  --delay             API 호출 간 대기 초 (기본: 0.5)
  --force             이미 수집된 데이터도 재수집

collect.py search
  --name              기업명 검색어
  --stock-code        종목코드
  --refresh           기업코드 XML 새로 다운로드
  --limit             최대 검색 결과 수 (기본: 20)
```

---

### 보조 스크립트

기업 목록을 직접 생성하거나 관리할 때 사용하는 개별 스크립트입니다.  
`run_pipeline.py`는 이 중 `f_fetch_induty_codes.py`만 캐시 생성 시 호출하며, 나머지는 필요 시 수동 실행합니다.

| 스크립트 | 역할 |
|---|---|
| `f_make_A_input.py` | KRX 엑셀에서 정상 기업 목록 CSV를 생성하는 보조 스크립트 |
| `f_make_delisted_input.py` | 상폐 기업 목록 CSV를 생성하는 보조 스크립트 |
| `f_fetch_induty_codes.py` | DART API로 상폐 기업의 업종코드를 조회하여 `data/etc/delisted_induty_codes.csv` 캐시 생성 |
| `f_split_companies.py` | 기업 목록 CSV를 여러 파일로 분할 (대규모 수집 시 활용) |

---

## 결과 데이터

### 재무비율 CSV (`data/output/`)

```
data/output/
├── Information Technology/
│   ├── 005930_2023.csv      # 삼성전자 2023년 (Q1, H1, Q3, ANNUAL)
│   └── 035420_2023.csv      # NAVER 2023년
├── Communication Services/
│   └── 035720_2023.csv      # 카카오 2023년
├── Consumer Discretionary/
│   └── 090430_2023.csv
└── ...
```

**CSV 컬럼:**

| 컬럼 | 설명 |
|---|---|
| `stock_code` | 종목코드 |
| `corp_name` | 기업명 |
| `year` | 연도 |
| `quarter` | 분기 (Q1/H1/Q3/ANNUAL) |
| `label` | 정상(0) / 상폐(1) |
| 총자산증가율 ~ 총자본투자효율 | 30개 재무비율 값 |

### 원본 재무제표 JSON (`data/raw/`)

`--save-raw` 옵션 사용 시 DART 원본 응답을 보존합니다.

```
data/raw/
├── 005930_2023_Q1_CFS.json
├── 005930_2023_H1_CFS.json
└── ...
```

### S3 저장 구조

```
s3://{bucket}/
├── healthy/
│   └── Information Technology/
│       ├── 005930_2023_Q1.json
│       └── ...
├── delisted/
│   └── Information Technology/
│       ├── 054630_2020_Q1.json
│       └── ...
```

---

## 30개 재무비율 목록

| # | 카테고리 | 비율명 | 산출식 |
|---|---|---|---|
| 1 | 성장성 | 총자산증가율 | (기말총자산 − 기초총자산) / 기초총자산 × 100 |
| 2 | 성장성 | 유동자산증가율 | (기말유동자산 − 기초유동자산) / 기초유동자산 × 100 |
| 3 | 성장성 | 매출액증가율 | (당기매출액 − 전기매출액) / 전기매출액 × 100 |
| 4 | 성장성 | 순이익증가율 | (당기순이익 − 전기순이익) / 전기순이익 × 100 |
| 5 | 성장성 | 영업이익증가율 | (당기영업이익 − 전기영업이익) / 전기영업이익 × 100 |
| 6 | 수익성 | 매출액순이익률 | 순이익 / 매출액 × 100 |
| 7 | 수익성 | 매출총이익률 | 매출총이익 / 매출액 × 100 |
| 8 | 수익성 | 자기자본순이익률 (ROE) | 순이익 / 자기자본 × 100 |
| 9 | 활동성 | 매출채권회전율 | 매출액 / 매출채권 |
| 10 | 활동성 | 재고자산회전율 | 매출원가 / 재고자산 |
| 11 | 활동성 | 총자본회전율 | 매출액 / 총자본 |
| 12 | 활동성 | 유형자산회전율 | 매출액 / 총자산 |
| 13 | 활동성 | 매출원가율 | 매출원가 / 매출액 × 100 |
| 14 | 안정성 | 부채비율 | 부채 / 자기자본 × 100 |
| 15 | 안정성 | 유동비율 | 유동자산 / 유동부채 × 100 |
| 16 | 안정성 | 자기자본비율 | 자기자본 / 총자산 × 100 |
| 17 | 안정성 | 당좌비율 | (유동자산 − 재고자산) / 유동부채 × 100 |
| 18 | 안정성 | 비유동자산장기적합률 | 비유동자산 / 장기차입금 |
| 19 | 안정성 | 순운전자본비율 | (유동자산 − 유동부채) / 총자본 × 100 |
| 20 | 안정성 | 차입금의존도 | (장기+단기차입금+사채) / 총자본 × 100 |
| 21 | 안정성 | 현금비율 | 현금예금 / 유동부채 × 100 |
| 22 | 안정성 | 유형자산 | 유형자산 절대값 |
| 23 | 안정성 | 무형자산 | 무형자산 절대값 |
| 24 | 안정성 | 무형자산상각비 | CF에서 추출 |
| 25 | 안정성 | 유형자산상각비 | CF에서 추출 |
| 26 | 안정성 | 감가상각비 | 유형자산상각비 + 무형자산상각비 |
| 27 | 가치평가 | 총자본영업이익률 | 영업이익 / 총자본 × 100 |
| 28 | 가치평가 | 총자본순이익률 | 당기순이익 / 총자본 × 100 |
| 29 | 가치평가 | 유보액/납입자본비율 | (이익잉여금+자본잉여금) / 납입자본금 × 100 |
| 30 | 가치평가 | 총자본투자효율 | (당기순이익 + 이자비용) / 총자본 |

---

## 핵심 모듈 설명

### `src/dart_api.py` — OpenDART API 클라이언트

| 함수 | 역할 |
|---|---|
| `fetch_financial_statements()` | 특정 기업/연도/분기의 전체 재무제표 JSON 조회 |
| `fetch_all_quarters()` | 한 연도의 모든 분기 재무제표를 한 번에 가져오기 |
| `resolve_corp_code()` | 종목코드(6자리) → DART 고유코드(8자리) 변환 |
| `find_corp()` | 기업명/종목코드로 DART 고유코드 검색 |

### `src/account_mapper.py` — 계정과목명 매핑

OpenDART에서 반환하는 계정과목명이 기업마다 다른 문제를 정규식 패턴 매칭으로 해결합니다.

```
A기업: "매출액"  /  B기업: "수익(매출액)"  /  C기업: "영업수익"
→ 모두 'revenue'로 통합
```

### `src/ratio_calculator.py` — 30개 재무비율 계산기

표준 키로 변환된 재무 항목을 받아서 30개 비율을 계산합니다.

### `src/collector.py` — 데이터 수집 오케스트레이터

- CFS → OFS 자동 폴백 (연결재무제표 없으면 별도재무제표로 재시도)
- API 호출 간 0.5초 대기 (분당 호출 제한 방지)
- UTF-8 BOM 포함 CSV 저장 (엑셀 한글 깨짐 방지)

---

## 참고 사항

- OpenDART API는 **분당 호출 횟수 제한**이 있습니다. `--delay` 옵션으로 호출 간격을 조절하세요.
- 일부 기업은 연결재무제표(CFS)가 없을 수 있으며, 자동으로 별도재무제표(OFS)로 전환됩니다.
- 비율 값이 `None`인 경우는 해당 계정과목이 재무제표에 존재하지 않거나 0 나눗셈인 경우입니다.
- 수집 연도 범위: 정상 기업 2015~2025, 상폐 기업은 폐지 연도에 맞춰 자동 조정됩니다.
- `data/etc/` 디렉터리는 `.gitignore`에 포함되어 있으므로 엑셀 파일은 각자 수동으로 배치해야 합니다.
