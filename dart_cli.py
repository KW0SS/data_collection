import argparse
from pathlib import Path

from dart_client import (
    DEFAULT_CORP_XML,
    DartApiError,
    download_corp_codes,
    find_corp,
    fetch_financial_statements,
    get_api_key,
    save_json,
)


def cmd_corp_code(args: argparse.Namespace) -> int:
    api_key = get_api_key(args.api_key)
    xml_path = Path(args.xml_path)
    if args.refresh or not xml_path.exists():
        download_corp_codes(api_key, xml_path)
        print(f"Saved corp codes to {xml_path}")

    results = find_corp(
        corp_name=args.corp_name,
        stock_code=args.stock_code,
        xml_path=xml_path,
        limit=args.limit,
    )
    if not results:
        print("No matching corp found.")
        return 0

    for row in results:
        print(
            f"{row['corp_code']}\t{row['corp_name']}\t"
            f"{row['stock_code']}\t{row['modify_date']}"
        )
    return 0


def cmd_financials(args: argparse.Namespace) -> int:
    api_key = get_api_key(args.api_key)

    corp_code = args.corp_code
    if not corp_code and args.corp_name:
        xml_path = Path(DEFAULT_CORP_XML)
        if not xml_path.exists():
            download_corp_codes(api_key, xml_path)
        results = find_corp(corp_name=args.corp_name, xml_path=xml_path, limit=1)
        if not results:
            raise DartApiError("Could not resolve corp_name to corp_code.")
        corp_code = results[0]["corp_code"]

    if not corp_code:
        raise DartApiError("corp_code is required (or provide corp_name).")

    payload = fetch_financial_statements(
        api_key=api_key,
        corp_code=corp_code,
        bsns_year=args.year,
        reprt_code=args.report,
        fs_div=args.fs_div,
    )

    if args.out:
        save_json(payload, Path(args.out))
        print(f"Saved response to {args.out}")
    else:
        print(payload)

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="OpenDART financial statement fetcher"
    )
    parser.add_argument("--api-key", help="DART API key (or env DART_API_KEY)")

    sub = parser.add_subparsers(dest="command", required=True)

    corp = sub.add_parser("corp-code", help="Download/search corp codes")
    corp.add_argument("--refresh", action="store_true", help="Redownload corp codes")
    corp.add_argument("--corp-name", help="Company name to search")
    corp.add_argument("--stock-code", help="Stock code to match exactly")
    corp.add_argument("--limit", type=int, default=20, help="Max rows to show")
    corp.add_argument(
        "--xml-path",
        default=str(DEFAULT_CORP_XML),
        help="Path for corpCode.xml",
    )
    corp.set_defaults(func=cmd_corp_code)

    fin = sub.add_parser("financials", help="Fetch financial statements")
    fin.add_argument("--corp-code", help="DART corp_code")
    fin.add_argument("--corp-name", help="Company name (uses local corpCode.xml)")
    fin.add_argument("--year", required=True, help="Business year, e.g. 2023")
    fin.add_argument(
        "--report",
        required=True,
        help="Report code (11013=Q1, 11012=half, 11014=Q3, 11011=annual)",
    )
    fin.add_argument(
        "--fs-div",
        default="CFS",
        choices=["CFS", "OFS"],
        help="CFS for consolidated or OFS for separate",
    )
    fin.add_argument("--out", help="Output JSON file path")
    fin.set_defaults(func=cmd_financials)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except DartApiError as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
