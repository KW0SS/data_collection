OpenDART 재무제표 CLI

사전 준비
- Python 3.10+
- OpenDART API 키
- (권장) 인증서 문제 방지를 위해 `certifi` 설치:
  pip3 install certifi

설정
1) `.env` 파일에 키를 추가:
   DART_API_KEY=YOUR_KEY

사용법
0) `main.py`로 실행:
   python main.py --help

1) 고유번호 다운로드(최초 1회) 및 검색:
   python dart_cli.py corp-code --refresh --corp-name "Samsung"

2) 재무제표 조회:
   python dart_cli.py financials --corp-code 00126380 --year 2023 --report 11011 --fs-div CFS --out data/samsung_2023_annual.json

메모
- 보고서 코드: 11013=1분기, 11012=반기, 11014=3분기, 11011=사업보고서
- fs-div: CFS(연결) 또는 OFS(개별)
