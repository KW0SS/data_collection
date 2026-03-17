# feat: 상폐 기업 데이터 수집 확장 및 2015년 이전 legacy 수집 워크플로우 추가

> Closes #5

## Summary

- 상폐 기업 데이터 수집 범위를 확장하고, 2015년 이전(legacy) 누락 데이터를 점검/수집하는 워크플로우를 추가
- 변경 규모: **211 files changed**, +1,650 / -10 (신규 결과 CSV 203개 포함)

## Changes

### CLI 서브커맨드 추가 (`collect.py`)

| 커맨드 | 설명 |
|--------|------|
| `check-missing` | `companies_collected.csv` vs `data/output/` 비교하여 2015년 이전 누락 기업/연도 목록 CSV 생성 |
| `collect-legacy` | 누락 목록 CSV 기반으로 `dart-fss`를 사용한 legacy 데이터 수집 실행 |

### 신규 모듈: `src/dart_legacy_fetcher.py`

- DART OpenAPI가 2015년 이후만 지원하므로, `dart-fss` 라이브러리를 통해 XBRL/HTML 원문에서 재무제표 추출
- 추출 결과를 기존 `account_mapper` -> `ratio_calculator` 파이프라인과 호환되는 형식으로 변환

### `src/collector.py` 확장

- 연도별 legacy / non-legacy 수집 경로 자동 분기 (`LEGACY_CUTOFF_YEAR = 2015`)
- legacy는 ANNUAL 단위 수집, CFS -> OFS 폴백 적용
- 재무비율 값이 전부 비어있는 경우 CSV 생성 스킵
- `generate_missing_legacy_csv()` 함수 추가

### `src/dart_api.py`

- `_http_get()`에서 네트워크 오류(`URLError`)를 `DartApiError`로 변환하여 프로그램 크래시 방지

### 데이터 추가 (23개 기업)

| 종목코드 | 기업명 | 섹터 | 수집 범위 |
|----------|--------|------|-----------|
| 065560 | 녹원씨엔아이 | Materials | 2011-2025 |
| 006580 | 대양제지 | Materials | 2011-2025 |
| 058220 | 아리온 | Information Technology | 2011-2025 |
| 115960 | 연우 | Materials | 2015-2025 |
| 148140 | 비디아이 | Industrials | 2017-2025 |
| 048260 | 오스템임플란트 | Health Care | 2007-2025 |
| 050540 | 엠피씨플러스 | Industrials | 2005-2025 |
| 064510 | 유니코 | Industrials | 2018-2025 |
| 126870 | 뉴로스 | Industrials | 2012-2025 |
| 050320 | 에스에이치엔엘 | Industrials | 2002-2025 |
| 053660 | 현진소재 | Materials | 2002-2025 |
| 221610 | 자안바이오 | Materials | 2016-2025 |
| 045890 | GV | Industrials | 2005-2025 |
| 087730 | 이엠네트웍스 | Materials | 2008-2025 |
| 141070 | 맥스로텍 | Industrials | 2012-2025 |
| 033430 | 디에스티 | Industrials | 1998-2025 |
| 065620 | 제낙스 | Materials | 2002-2025 |
| 102210 | 에이치디 | Industrials | 2009-2025 |
| 028040 | 미래SCI | Industrials | 1996-2025 |
| 065940 | 바이오빌 | Materials | 2003-2025 |
| 112240 | 에스에프씨 | Materials | 2011-2025 |
| 122800 | 썬텍 | Materials | 2010-2025 |
| 065160 | 에프티이앤이 | Materials | 2002-2025 |

## Next Steps

- [ ] `check-missing` 기준 legacy 누락 연도 보강 수집
- [ ] `collect-legacy` / `collect` 실행 경로 스모크 테스트 추가
- [ ] `dart-fss`, `boto3` 의존성 및 실행 환경(venv) 가이드 README 반영
- [ ] `companies_template.csv` 기준 추가 기업 수집 및 결과 검증
