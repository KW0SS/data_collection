# 상폐 예측 모델용 재무비율 데이터 수집 도구

코스닥 정상/상폐 기업의 **분기별 재무비율 30개**를 OpenDART API로 수집하여 CSV로 저장하는 도구입니다.

---

## 프로젝트 구조

```
data_collection/
├── collect.py                      # CLI 진입점 (사용자가 실행하는 파일)
├── src/
│   ├── __init__.py
│   ├── dart_api.py                 # OpenDART API 클라이언트
│   ├── account_mapper.py           # DART 계정과목명 → 표준 키 매핑
│   ├── ratio_calculator.py         # 30개 재무비율 계산기
│   ├── collector.py                # 데이터 수집 오케스트레이터
│   └── s3_uploader.py              # S3 업로드 모듈 (GICS 섹터별)
├── data/
│   ├── input/                      # 기업 목록 CSV
│   │   ├── companies_template.csv  # 수집 대기열 (실행 후 비우고 재사용)
│   │   └── companies_collected.csv # 수집 완료 기록 (자동 누적)
│   ├── output/                     # 결과 재무비율 CSV (GICS 섹터별)
│   └── raw/                        # 원본 재무제표 JSON (선택)
├── requirements.txt
└── .env                            # API 키 및 S3 설정
```

---

## 사전 준비

- **Python** 3.10 이상
- **OpenDART API 키** ([https://opendart.fss.or.kr](https://opendart.fss.or.kr) 에서 발급)
- (권장) 인증서 문제 방지를 위해 certifi 설치:
  ```bash
  pip3 install certifi
  ```

## 설정

`.env` 파일을 프로젝트 루트에 생성하고 아래 설정을 추가합니다:

```
DART_API_KEY=발급받은_API_키

# S3 업로드 기능 사용 시 (--upload-s3)
S3_ACCESS_KEY=AWS_Access_Key_ID
S3_PRIVATE_KEY=AWS_Secret_Access_Key
S3_BUCKET_NAME=my-bucket-name
S3_REGION=ap-northeast-2          # 선택 (기본: ap-northeast-2)
```

---

## 사용법

### 1) 종목코드로 직접 수집

```bash
# 단일 기업, 특정 연도 → 019440_2023.csv 생성
python3 collect.py collect --stock-codes 019440 --years 2023

# 복수 기업, 복수 연도 → 019440_2020.csv ~ 005930_2023.csv 등 연도별 파일 생성
python3 collect.py collect --stock-codes 019440 005930 --years 2020 2021 2022 2023

# 특정 분기만 지정
python3 collect.py collect --stock-codes 019440 --years 2023 --quarters Q1 ANNUAL

# 저장 디렉터리 지정
python3 collect.py collect --stock-codes 019440 --years 2023 -o data/output/my_folder/

# 원본 재무제표 JSON도 함께 저장
python3 collect.py collect --stock-codes 019440 --years 2023 --save-raw

# S3에 GICS 섹터별로 원본 데이터 업로드 (기업 목록 CSV에 gics_sector 필요)
python3 collect.py collect --companies data/input/companies.csv --years 2023 --upload-s3

# S3 버킷 직접 지정
python3 collect.py collect --companies data/input/companies.csv --years 2023 --upload-s3 --s3-bucket my-bucket
```

**파일명 규칙:** `{GICS섹터}/{종목코드}_{연도}.csv` (예: `Materials/019440_2023.csv`)

### 2) 기업 목록 CSV로 배치 수집

`data/input/companies.csv` 작성:

```csv
stock_code,corp_name,label,gics_sector,start_year,end_year
019440,세아특수강,1,Materials,2020,2023
005930,삼성전자,0,Information Technology,2021,2024
035720,카카오,0,Communication Services,,
```

| 컬럼 | 필수 | 설명 |
|---|---|---|
| `stock_code` | ✅ | 종목코드 (6자리) |
| `corp_name` | ✅ | 기업명 (참고용) |
| `label` | ✅ | 0=정상, 1=상폐 (모델 학습용 라벨) |
| `gics_sector` | ⬜ | GICS 섹터명 (S3 업로드 시 디렉터리 구분에 사용) |
| `start_year` | ⬜ | 수집 시작 연도 (기업별 개별 지정, 없으면 `--years` 사용) |
| `end_year` | ⬜ | 수집 종료 연도 (`start_year`와 함께 사용) |

> **기업별 연도 범위:** 기업마다 상장 기간이 다르므로, CSV에 `start_year`/`end_year`를 기업별로 지정하면 해당 범위만 수집합니다. 지정하지 않은 기업은 CLI의 `--years` 값을 사용합니다.

```bash
# CSV에 start_year/end_year가 있으면 --years는 미지정 기업에만 적용됨
python3 collect.py collect --companies data/input/companies.csv --years 2023
```

### 3) 기업 검색 (DART 고유코드 조회)

```bash
python3 collect.py search --name 세아특수강
python3 collect.py search --stock-code 019440
python3 collect.py search --name 삼성 --limit 10
```

### 전체 CLI 옵션

```
collect.py collect
  --stock-codes       종목코드 목록 (예: 019440 005930)
  --companies         기업 목록 CSV 파일 경로
  --years             수집 연도 (기본: 2023)
  --quarters          수집 분기 (Q1, H1, Q3, ANNUAL / 기본: 전체)
  --fs-div            CFS=연결재무제표, OFS=별도재무제표 (기본: CFS)
  --output-dir, -o    결과 CSV 저장 디렉터리 (기본: data/output/)
  --save-raw          원본 재무제표 JSON을 data/raw/에 저장
  --upload-s3         원본 재무제표 JSON을 S3에 GICS 섹터별로 업로드
  --s3-bucket         S3 버킷 이름 (없으면 .env의 S3_BUCKET_NAME)
  --s3-region         AWS 리전 (없으면 .env의 S3_REGION / 기본: ap-northeast-2)
  --delay             API 호출 간 대기 초 (기본: 0.5)

collect.py search
  --name              기업명 검색어
  --stock-code        종목코드
  --refresh           기업코드 XML 새로 다운로드
  --limit             최대 검색 결과 수 (기본: 20)
```

**파일 저장 구조 예시:**

```
data/output/
├── Health Care/
│   ├── 052670_2010.csv     # 제일바이오 2010년 (Q1, H1, Q3, ANNUAL)
│   ├── 052670_2024.csv     # 제일바이오 2024년
│   └── 150840_2021.csv     # 인트로메딕 2021년
├── Materials/
│   ├── 019440_2022.csv     # 세아특수강 2022년
│   └── 019440_2023.csv     # 세아특수강 2023년
├── Information Technology/
│   └── 054630_2023.csv     # 에이디칩스 2023년
├── Industrials/
│   └── 024810_2023.csv     # 이화전기 2023년
└── ...
```

> ⚠️ `gics_sector`가 지정되지 않은 기업은 `Unknown/` 디렉터리에 저장됩니다.

---

## 데이터 흐름

```
사용자 입력 (종목코드 + 연도)
      │
      ▼
┌─────────────┐     ┌──────────────┐     ┌───────────────┐     ┌──────────┐
│  collect.py  │ ──▶ │  collector.py │ ──▶ │ account_mapper│ ──▶ │  ratio   │
│  (CLI 진입)  │     │  (수집 조율)  │     │ (계정명 매핑)  │     │calculator│
└─────────────┘     └──────────────┘     └───────────────┘     └──────────┘
                          │                                          │
                          ▼                                          ▼
                    ┌────────────┐                           결과 CSV 저장
                    │  dart_api  │                        (data/output/*.csv)
                    │ (API 호출)  │
                    └────────────┘
                          │
                          ▼
                    OpenDART 서버
```

---

## 파일별 상세 설명

### `collect.py` — CLI 진입점

사용자가 직접 실행하는 파일입니다. `argparse`로 CLI 인자를 파싱하고 두 개의 서브커맨드를 제공합니다:

| 커맨드 | 함수 | 역할 |
|---|---|---|
| `collect` | `cmd_collect()` | 재무비율 데이터 수집 → CSV 저장 |
| `search` | `cmd_search()` | DART 기업코드 검색 (종목코드/기업명) |

---

### `src/dart_api.py` — OpenDART API 클라이언트

OpenDART 서버와 직접 통신하는 유일한 모듈입니다.

| 함수 | 역할 |
|---|---|
| `get_api_key()` | `.env` 또는 환경변수에서 DART API 키 읽기 |
| `_http_get()` | HTTP GET 요청 (SSL 인증서 처리 포함) |
| `download_corp_codes()` | DART에서 전체 기업코드 XML 다운로드 |
| `load_corp_codes()` | XML 파싱 → 기업 리스트 변환 |
| `find_corp()` | 기업명/종목코드로 DART 고유코드 검색 |
| `resolve_corp_code()` | 종목코드(6자리) → DART 고유코드(8자리) 변환 |
| `fetch_financial_statements()` | **핵심** — 특정 기업·연도·분기의 전체 재무제표 JSON 조회 |
| `fetch_all_quarters()` | 한 연도의 모든 분기 재무제표를 한 번에 가져오기 |

**보고서 코드:**

| 코드 | 분기 | REPORT_CODES 키 |
|---|---|---|
| 11013 | 1분기보고서 | `Q1` |
| 11012 | 반기보고서 | `H1` |
| 11014 | 3분기보고서 | `Q3` |
| 11011 | 사업보고서 | `ANNUAL` |

---

### `src/account_mapper.py` — 계정과목명 매핑

**문제:** OpenDART에서 반환하는 계정과목명(account_nm)이 기업마다 다릅니다.

```
A기업: "매출액"
B기업: "수익(매출액)"
C기업: "영업수익"
→ 모두 같은 '매출(revenue)'이지만 이름이 다름
```

**해결:** 정규식 패턴 매칭으로 다양한 이름을 하나의 표준 키에 통합합니다.

| 재무제표 | 표준 키 | 한글명 | 매칭 패턴 예시 |
|---|---|---|---|
| BS (재무상태표) | `total_assets` | 자산총계 | `자산\s*총계` |
| BS | `current_assets` | 유동자산 | `유동\s*자산$` |
| BS | `trade_receivables` | 매출채권 | `매출\s*채권\|단기매출채권` |
| BS | `cash` | 현금및현금성자산 | `현금\s*(및\|과)\s*현금\s*성?\s*자산` |
| IS (손익계산서) | `revenue` | 매출액 | `^매출액$\|^수익\s*\(매출액\)$\|^영업\s*수익$` |
| IS | `net_income` | 당기순이익 | `당기\s*순이익\|당기순이익` |
| CF (현금흐름표) | `depreciation` | 감가상각비 | `유형\s*자산\s*감가\s*상각비` |
| ... | ... | ... | ... |

**전체 표준 키 목록 (25개):**

- **BS (17개):** `total_assets`, `current_assets`, `non_current_assets`, `tangible_assets`, `intangible_assets`, `trade_receivables`, `inventories`, `cash`, `total_liabilities`, `current_liabilities`, `short_term_borrowings`, `long_term_borrowings`, `bonds_payable`, `total_equity`, `paid_in_capital`, `retained_earnings`, `capital_surplus`
- **IS (6개):** `revenue`, `cost_of_sales`, `gross_profit`, `operating_income`, `net_income`, `interest_expense`
- **CF (2개):** `depreciation`, `amortization`

`extract_standard_items()` 함수가 DART 원시 데이터를 받아서 표준 형태로 변환합니다:

```
입력: [{"account_nm": "자산총계", "thstrm_amount": "500,000,000", ...}, ...]
출력: {"total_assets": {"thstrm": 500000000.0, "frmtrm": 480000000.0, ...}}
```

- `thstrm` = 당기(현재 기간) 금액
- `frmtrm` = 전기(이전 기간) 금액
- `bfefrmtrm` = 전전기 금액

---

### `src/ratio_calculator.py` — 30개 재무비율 계산기

표준 키로 변환된 재무 항목을 받아서 30개 비율을 계산합니다.

**유틸리티 함수:**

| 함수 | 역할 |
|---|---|
| `_get(items, key, period)` | 특정 항목·기간 값 추출 |
| `_safe_div(a, b)` | 0 나눗셈·None 안전 처리 |
| `_pct(a, b)` | `(a / b) * 100` 비율 계산 |
| `_growth(curr, prev)` | `(당기 - 전기) / 전기 * 100` 증가율 |

**30개 재무비율 목록:**

| # | 카테고리 | 비율명 | 산출식 |
|---|---|---|---|
| 1 | 성장성 | 총자산증가율 | (기말총자산 − 기초총자산) / 기초총자산 × 100 |
| 2 | 성장성 | 유동자산증가율 | (기말유동자산 − 기초유동자산) / 기초유동자산 × 100 |
| 3 | 성장성 | 매출액증가율 | (당기매출액 − 전기매출액) / 전기매출액 × 100 |
| 4 | 성장성 | 순이익증가율 | (당기순이익 − 전기순이익) / 전기순이익 × 100 |
| 5 | 성장성 | 영업이익증가율 | (당기영업이익 − 전기영업이익) / 전기영업이익 × 100 |
| 6 | 수익성 | 매출액순이익률 | 순이익 / 매출액 × 100 |
| 7 | 수익성 | 매출총이익률 | 매출총이익 / 매출액 × 100 |
| 8 | 수익성 | 자기자본순이익률 | 순이익 / 자기자본 × 100 (ROE) |
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

### `src/collector.py` — 데이터 수집 오케스트레이터

전체 수집 과정을 조율하는 핵심 모듈입니다.

**`collect_single()`** — 기업 1개 × 분기 1개 처리:

```
종목코드 019440, 2020년, Q1
   │
   ├── 1) dart_api.fetch_financial_statements() → 원시 재무제표
   ├── 2) account_mapper.extract_standard_items() → 표준 키 변환
   └── 3) ratio_calculator.compute_all_ratios()  → 30개 비율 계산
```

**`collect_batch()`** — 여러 기업 × 여러 연도 × 여러 분기 배치 처리:

핵심 동작:
- **CFS → OFS 자동 폴백:** 연결재무제표(CFS)가 없는 기업은 자동으로 별도재무제표(OFS)로 재시도
- **API 호출 제한 관리:** 호출 간 0.5초 대기 (OpenDART 분당 호출 제한 방지)
- **진행률 표시:** `[3/16] 세아특수강 2020-Q3 ...` 형태로 진행 상황 출력
- **CSV 저장:** UTF-8 BOM 포함 (엑셀에서 한글이 깨지지 않음)

---

## 결과 CSV 형식

| 컬럼 | 설명 |
|---|---|
| `stock_code` | 종목코드 |
| `corp_name` | 기업명 |
| `year` | 연도 |
| `quarter` | 분기 (Q1/H1/Q3/ANNUAL) |
| `label` | 정상(0) / 상폐(1) |
| 총자산증가율 ~ 총자본투자효율 | 30개 재무비율 값 |

---

## 원본 재무제표 저장 (--save-raw)

`--save-raw` 옵션을 사용하면 OpenDART에서 받은 원본 재무제표 JSON을 `data/raw/` 폴더에 보존합니다.

```bash
python3 collect.py collect --stock-codes 019440 --years 2023 --save-raw
```

저장 경로: `data/raw/{종목코드}_{연도}_{분기}.json`

원본 데이터를 보존하면 다음과 같은 장점이 있습니다:
- **디버깅:** 비율 값이 이상할 때 원본 데이터를 확인하여 원인 파악 가능
- **재현성:** 나중에 비율 산출 공식을 변경하더라도 API를 다시 호출하지 않고 재계산 가능
- **확장성:** 현재 30개 비율 외에 추가 분석이 필요할 때 원본에서 바로 추출 가능
- **학술 연구:** 데이터의 출처와 근거를 명확히 남길 수 있음

---

## S3 업로드 (--upload-s3)

`--upload-s3` 옵션을 사용하면 수집된 원본 재무제표 JSON을 AWS S3에 **GICS 섹터별 디렉터리**로 자동 업로드합니다.

### 사전 준비

1. `.env`에 S3 인증 정보 추가:
   ```
   S3_ACCESS_KEY=AKIA...
   S3_PRIVATE_KEY=wJal...
   S3_BUCKET_NAME=my-financial-data
   S3_REGION=ap-northeast-2
   ```
2. boto3 설치: `pip3 install boto3`
3. 기업 목록 CSV에 `gics_sector` 컬럼 추가

### S3 저장 구조

```
s3://my-financial-data/
├── Materials/
│   ├── 019440_2023_Q1.json
│   ├── 019440_2023_H1.json
│   ├── 019440_2023_Q3.json
│   └── 019440_2023_ANNUAL.json
├── Information Technology/
│   ├── 005930_2023_Q1.json
│   └── ...
└── Communication Services/
    ├── 035720_2023_Q1.json
    └── ...
```

### 동작 방식

- S3에 지정한 버킷이 없으면 **자동 생성**
- S3는 "디렉터리"가 아닌 key prefix로 동작하므로, 섹터 폴더도 **자동 생성**됨
- 재무제표 데이터가 없는 분기(API 응답 빈 리스트)는 업로드하지 않음
- `--upload-s3`와 `--save-raw`는 독립적으로 사용 가능 (동시 사용도 가능)

---

## 기업 선정 가이드

### 상폐 기업 (label=1)

직접 찾는 것을 권장합니다. 이유:
1. **상폐 사유 구분 필요:** 자진상폐, 합병, 재무적 상폐 등 사유가 다양하며, 예측 모델에는 **재무적 사유로 상폐된 기업**만 선별해야 의미가 있음
2. **상폐 시점 파악:** 상폐 직전 N년간의 데이터가 필요하므로, 상폐 시점을 정확히 알아야 적절한 연도 범위를 설정 가능
3. **참고 소스:** KRX KIND ([https://kind.krx.co.kr](https://kind.krx.co.kr)), 한국거래소 상장폐지종목 목록

### 정상 기업 (label=0)

비교 그룹(control group) 선정 시 **업종·규모 매칭**이 모델 품질에 큰 영향을 미칩니다.

---

## 참고 사항

- OpenDART API는 **분당 호출 횟수 제한**이 있습니다. `--delay` 옵션으로 호출 간격을 조절하세요.
- 일부 기업은 연결재무제표(CFS)가 없을 수 있습니다. 이 경우 자동으로 별도재무제표(OFS)로 전환됩니다.
- 비율 값이 `None`인 경우는 해당 계정과목이 재무제표에 존재하지 않거나, 0 나눗셈인 경우입니다.
