# A_companies_final.csv를 정상/상폐 기업으로 분리
import pandas as pd
from pathlib import Path

INPUT_FILE   = Path("data/input/A_companies_final.csv")
NORMAL_FILE  = Path("data/input/A_normal.csv")
DELISTED_FILE= Path("data/input/A_delisted.csv")

df = pd.read_csv(INPUT_FILE, dtype={"stock_code": str})

df_normal   = df[df["label"] == 0].reset_index(drop=True)
df_delisted = df[df["label"] == 1].reset_index(drop=True)

df_normal.to_csv(NORMAL_FILE,   index=False, encoding="utf-8-sig")
df_delisted.to_csv(DELISTED_FILE, index=False, encoding="utf-8-sig")

print(f"정상 기업:  {len(df_normal)}개  → {NORMAL_FILE}")
print(f"상폐 기업:  {len(df_delisted)}개  → {DELISTED_FILE}")
print()
print("=== 정상 기업 GICS 분포 ===")
print(df_normal["gics_sector"].value_counts())
print()
print("=== 상폐 기업 GICS 분포 ===")
print(df_delisted["gics_sector"].value_counts())
print()
print("수집 명령어:")
print(f"  python f_collect.py collect --companies {NORMAL_FILE} --save-raw")
print(f"  python f_collect.py collect --companies {DELISTED_FILE} --save-raw")

# # 정상 기업 수집
# python f_collect.py collect --companies data/input/A_normal.csv --save-raw

# # 상폐 기업 수집
# python f_collect.py collect --companies data/input/A_delisted.csv --save-raw