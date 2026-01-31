import json
import os
import ssl
import urllib.parse
import urllib.request
import zipfile
from io import BytesIO
from pathlib import Path
import xml.etree.ElementTree as ET

BASE_URL = "https://opendart.fss.or.kr/api"
CORP_CODE_ENDPOINT = f"{BASE_URL}/corpCode.xml"
FIN_STATEMENTS_ENDPOINT = f"{BASE_URL}/fnlttSinglAcntAll.json"

DEFAULT_DATA_DIR = Path("data")
DEFAULT_CORP_XML = DEFAULT_DATA_DIR / "corpCode.xml"


class DartApiError(Exception):
    pass


def _read_env_file(path: Path) -> dict:
    env = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def get_api_key(explicit_key: str | None = None) -> str:
    if explicit_key:
        return explicit_key
    env = _read_env_file(Path(".env"))
    key = os.getenv("DART_API_KEY") or env.get("DART_API_KEY")
    if not key:
        raise DartApiError(
            "Missing API key. Set DART_API_KEY env var or add it to .env."
        )
    return key


def _http_get(url: str, params: dict, timeout: int = 30) -> bytes:
    query = urllib.parse.urlencode(params)
    req = urllib.request.Request(f"{url}?{query}", method="GET")
    context = None
    try:
        import certifi

        context = ssl.create_default_context(cafile=certifi.where())
    except Exception:
        context = None
    with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
        return resp.read()


def download_corp_codes(api_key: str, out_path: Path = DEFAULT_CORP_XML) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"crtfc_key": api_key}
    data = _http_get(CORP_CODE_ENDPOINT, payload)

    with zipfile.ZipFile(BytesIO(data)) as zf:
        # OpenDART provides a single XML file in the zip
        name = zf.namelist()[0]
        xml_bytes = zf.read(name)
        out_path.write_bytes(xml_bytes)

    return out_path


def load_corp_codes(xml_path: Path = DEFAULT_CORP_XML) -> list[dict]:
    if not xml_path.exists():
        raise DartApiError(
            f"Corp code XML not found at {xml_path}. "
            "Run the corp-code command with --refresh to download it."
        )
    tree = ET.parse(xml_path)
    root = tree.getroot()
    rows = []
    for node in root.findall("list"):
        rows.append(
            {
                "corp_code": (node.findtext("corp_code") or "").strip(),
                "corp_name": (node.findtext("corp_name") or "").strip(),
                "corp_eng_name": (node.findtext("corp_eng_name") or "").strip(),
                "stock_code": (node.findtext("stock_code") or "").strip(),
                "modify_date": (node.findtext("modify_date") or "").strip(),
            }
        )
    return rows


def find_corp(
    corp_name: str | None = None,
    stock_code: str | None = None,
    xml_path: Path = DEFAULT_CORP_XML,
    limit: int = 20,
) -> list[dict]:
    rows = load_corp_codes(xml_path)
    results = []
    name_q = (corp_name or "").lower()
    stock_q = (stock_code or "").lower()

    for row in rows:
        if name_q and name_q not in row["corp_name"].lower():
            continue
        if stock_q and stock_q != row["stock_code"].lower():
            continue
        results.append(row)
        if len(results) >= limit:
            break
    return results


def fetch_financial_statements(
    api_key: str,
    corp_code: str,
    bsns_year: str,
    reprt_code: str,
    fs_div: str,
) -> dict:
    payload = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "reprt_code": reprt_code,
        "fs_div": fs_div,
    }
    data = _http_get(FIN_STATEMENTS_ENDPOINT, payload)
    payload = json.loads(data.decode("utf-8"))
    status = payload.get("status")
    if status and status != "000":
        raise DartApiError(f"OpenDART error {status}: {payload.get('message')}")
    return payload


def save_json(payload: dict, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
