# 프로젝트 컨텍스트

DART 재무제표 수집 파이프라인 프로젝트. 종목코드별 재무비율 CSV를 `data/output/`에 저장하고, 원본 JSON을 `data/raw/`에 저장 후 S3에 업로드.

## PR 분석 워크플로우

사용자가 "PR 분석해줘", "PR 요약해줘" 등을 요청하면 다음 순서로 진행:

### 1단계: 파이프라인 실행 (기계적 점검)
```bash
python3 scripts/pr_pipeline.py --output-json prs/context.json
```
- `prs/` 디렉터리에 기본 PR 설명 마크다운이 생성됨
- `--output-json`으로 구조화된 분석 컨텍스트(커밋, 파일변경, 점검결과)가 JSON으로 저장됨

### 2단계: diff 분석 (에이전트)
- `prs/context.json`을 읽고, `git diff main...HEAD`로 실제 코드 변경을 확인
- 변경된 주요 파일들을 직접 읽어서 코드 의도 파악

### 3단계: PR 설명 작성
생성된 마크다운 파일의 "## 변경 요약" 섹션을 다음 구조로 채움:
- **변경 배경/동기**: 왜 이 변경이 필요했는지 (커밋 메시지 + 코드에서 추론)
- **주요 변경 사항**: 핵심 변경을 bullet point로 (무엇을, 왜, 어떻게)
- **주의할 점**: breaking change, 새 의존성, 설계 변경 등
- **영향 범위**: 기존 기능에 미치는 영향

### 작성 규칙
- 한국어로 작성
- 기술적이되 읽기 쉽게
- 파일 목록 나열 금지 (이미 별도 섹션에 있음)
- 코드 변경의 "의도"에 집중

### 옵션
- `--head-ref <branch>`: HEAD 대신 다른 브랜치와 비교
- `--type data|structure|both`: PR 타입 수동 지정
- `--create-pr`: GitHub PR 자동 생성
- `--draft`: 드래프트 PR로 생성
