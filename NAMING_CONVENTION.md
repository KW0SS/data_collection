# 파일 네이밍 컨벤션

---

## 출력 CSV 파일

**형식:** `{종목코드}_{연도}.csv`

```
019440_2022.csv     ← 세아특수강 2022년
019440_2023.csv     ← 세아특수강 2023년
005930_2023.csv     ← 삼성전자 2023년
```

| 구성 요소 | 형식 | 예시 |
|---|---|---|
| 종목코드 | 6자리 숫자 | `019440` |
| 연도 | 4자리 숫자 | `2023` |
| 구분자 | `_` (언더스코어) | |
| 확장자 | `.csv` | |

---

## 원본 재무제표 JSON (로컬)

**형식:** `{종목코드}_{연도}_{분기}_{재무제표구분}.json`

```
019440_2023_Q1_CFS.json
019440_2023_H1_CFS.json
019440_2023_Q3_OFS.json
019440_2023_ANNUAL_CFS.json
```

| 구성 요소 | 형식 | 값 |
|---|---|---|
| 종목코드 | 6자리 숫자 | `019440` |
| 연도 | 4자리 숫자 | `2023` |
| 분기 | 대문자 | `Q1`, `H1`, `Q3`, `ANNUAL` |
| 재무제표 구분 | 대문자 | `CFS` (연결), `OFS` (별도) |

---

## S3 업로드 경로

**형식:** `s3://{버킷}/{GICS섹터}/{종목코드}_{연도}_{분기}.json`

```
s3://kw0ss-raw-data-s3/
├── Materials/
│   ├── 019440_2023_Q1.json
│   └── 019440_2023_ANNUAL.json
├── Information Technology/
│   └── 005930_2023_Q1.json
└── Communication Services/
    └── 035720_2023_Q1.json
```

| 구성 요소 | 형식 | 설명 |
|---|---|---|
| GICS 섹터 | 영문 (아래 표 참조) | S3 prefix(디렉터리) 역할 |
| 파일명 | `{종목코드}_{연도}_{분기}.json` | |

---

## GICS 섹터 영문 명칭

S3 디렉터리명 및 기업 목록 CSV의 `gics_sector` 컬럼에 반드시 아래 **공식 영문 명칭**을 사용합니다.

| # | 섹터 코드 | 영문 명칭 (S3 디렉터리명) | 한글명 |
|---|---|---|---|
| 1 | 10 | `Energy` | 에너지 |
| 2 | 15 | `Materials` | 소재 |
| 3 | 20 | `Industrials` | 산업재 |
| 4 | 25 | `Consumer Discretionary` | 경기관련소비재 |
| 5 | 30 | `Consumer Staples` | 필수소비재 |
| 6 | 35 | `Health Care` | 헬스케어 |
| 7 | 40 | `Financials` | 금융 |
| 8 | 45 | `Information Technology` | 정보기술 |
| 9 | 50 | `Communication Services` | 커뮤니케이션서비스 |
| 10 | 55 | `Utilities` | 유틸리티 |
| 11 | 60 | `Real Estate` | 부동산 |

### 사용 예시 — 기업 목록 CSV

```csv
stock_code,corp_name,label,gics_sector
019440,세아특수강,1,Materials
005930,삼성전자,0,Information Technology
035720,카카오,0,Communication Services
003490,대한항공,0,Industrials
068270,셀트리온,0,Health Care
105560,KB금융,0,Financials
017670,SK텔레콤,0,Communication Services
015760,한국전력,0,Utilities
```

> ⚠️ `gics_sector` 값은 위 표의 **영문 명칭**을 정확히 사용해야 합니다. 오타나 다른 표기를 쓰면 S3에 별도 디렉터리가 생성됩니다.

---

## 입력 CSV 파일

**형식:** 역할을 나타내는 **snake_case** 이름

```
companies_template.csv      ← 기업 목록 템플릿
companies.csv               ← 실제 기업 목록 (사용자 작성)
```

---

## 문서 파일

**형식:** **UPPER_CASE**.md

```
README.md                   ← 프로젝트 메인 문서
DOPPLER_GUIDE.md            ← Doppler 설정 가이드
NAMING_CONVENTION.md        ← 네이밍 컨벤션 (이 파일)
```
