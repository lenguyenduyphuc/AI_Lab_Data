from pathlib import Path
import argparse
import json
import re
import sys
import pandas as pd

# ── cleaning helpers ──────────────────────────────────────────────────────
_FANCY_QUOTES = str.maketrans("‘’‚‛“”„‟‹›«»", "''''''''<<>>")
_RE_CONTROL      = re.compile(r"[\x00-\x1F\x7F]")           
_RE_BACKSLASH    = re.compile(r"\\+")                       
_RE_DASH_REPEAT  = re.compile(r"[-]{2,}")                 
_RE_SPACE_MULTI  = re.compile(r"\s{2,}")                   
_RE_SYMBOLS      = re.compile(r"[^\w\s]")                   

def clean(text: str) -> str:
    txt = str(text).translate(_FANCY_QUOTES)
    txt = _RE_BACKSLASH.sub(" ", txt)
    txt = txt.replace('"', "'")
    txt = _RE_CONTROL.sub(" ", txt)
    txt = _RE_DASH_REPEAT.sub(" - ", txt)
    txt = _RE_SYMBOLS.sub(" ", txt)          # ← key line
    txt = _RE_SPACE_MULTI.sub(" ", txt.strip())
    return txt

# ── write as pretty-printed JSON array ────────────────────────────────────
def write_json_array(df: pd.DataFrame, dest: Path, instruction: str) -> None:
    records = [
        {"instruction": instruction, "input": row["prompt"], "output": row["completion"]}
        for _, row in df.iterrows()
    ]
    dest.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")

# ── CLI ───────────────────────────────────────────────────────────────────
INPUT_FILE   = Path("mental_disorders_reddit_labelled_cp_20250609_212001.xlsx")
OUTPUT_FILE  = Path("guardiane_mental_disorders_7.json")
INSTRUCTION  = "Classify the text:"

def main() -> None:
    sheet = 0
    try:
        df = pd.read_excel(INPUT_FILE, sheet_name=sheet)
    except Exception as err:
        try:
            print("Available sheets:", pd.ExcelFile(INPUT_FILE).sheet_names, file=sys.stderr)
        except Exception:
            pass
        sys.exit(f"Failed to read Excel: {err}")

    required = {"body", "labels"}
    if not required.issubset(df.columns):
        sys.exit(f"Missing columns: {', '.join(required - set(df.columns))}")

    df = (
        df[["body", "labels"]]
        .rename(columns={"body": "prompt", "labels": "completion"})
        .dropna()
    )

    df["prompt"]    = df["prompt"].apply(clean)

    write_json_array(df, OUTPUT_FILE, INSTRUCTION)
    print(f"✅ Wrote {len(df):,} records → {OUTPUT_FILE.resolve()}")

if __name__ == "__main__":
    main()
