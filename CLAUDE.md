# CLAUDE.md

## 프로젝트
DART 재무제표 수집 파이프라인. `data/output/`에 재무비율 CSV, `data/raw/`에 원본 JSON 저장 후 S3 업로드.

## PR 분석 워크플로우

사용자가 "PR 분석해줘", "PR 요약해줘" 등을 요청하면 아래 순서로 진행한다.

### 1단계: 파이프라인 실행
```bash
python3 scripts/pr_pipeline.py --output-json prs/context.json
```
- `prs/` 디렉터리에 PR 설명 마크다운 생성
- `prs/context.json`에 구조화된 분석 컨텍스트 저장

### 2단계: diff 분석
- `prs/context.json` 읽기
- `git diff main..HEAD`로 실제 코드 변경 확인
- 변경된 주요 파일을 직접 읽어 코드 의도 파악

### 3단계: PR 설명 작성
생성된 `prs/*.md`의 "## 변경 요약" 섹션을 아래 구조로 채운다:
- **변경 배경/동기**: 왜 이 변경이 필요했는지 (커밋 메시지 + 코드에서 추론)
- **주요 변경 사항**: 핵심 변경을 bullet point로 (무엇을, 왜, 어떻게)
- **주의할 점**: breaking change, 새 의존성, 설계 변경 등
- **영향 범위**: 기존 기능에 미치는 영향

### 작성 규칙
- 한국어로 작성
- 기술적이되 읽기 쉽게
- 파일 목록 나열 금지 (별도 섹션에 존재)
- 코드 변경의 "의도"에 집중

### CLI 옵션
- `--head-ref <branch>`: HEAD 대신 다른 브랜치와 비교
- `--type data|structure|both`: PR 타입 수동 지정
- `--include-worktree`: 미커밋 변경 포함 (현재 체크아웃 브랜치만)
- `--create-pr`: GitHub PR 자동 생성
- `--draft`: 드래프트 PR로 생성

## 브랜치 비교

"브랜치 비교해줘", "main과 차이" 등을 요청하면:
1. `git rev-list --left-right --count main..HEAD`
2. `git diff --name-status main..HEAD`
3. 필요 시 `python3 scripts/pr_pipeline.py --dry-run`으로 점검 실행
4. 커밋 차이, 파일 변경 요약, 리스크 노트를 정리
