#!/usr/bin/env python3
"""Download OpenDART corp codes XML to data/etc/corp_codes.xml."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.dart_api import DartApiError, download_corp_codes, get_api_key


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download OpenDART corp codes XML for run_pipeline.",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="DART API key (default: env/.env DART_API_KEY)",
    )
    parser.add_argument(
        "--out",
        default="data/etc/corp_codes.xml",
        help="Output XML path (default: data/etc/corp_codes.xml)",
    )
    args = parser.parse_args()

    out_path = Path(args.out)

    try:
        api_key = get_api_key(args.api_key)
        saved = download_corp_codes(api_key, out_path=out_path)
    except DartApiError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1

    print(f"Saved: {saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
