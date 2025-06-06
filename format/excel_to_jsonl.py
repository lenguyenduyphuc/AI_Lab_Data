# running code 
# python excel_to_jsonl.py Human_checked.xlsx --sheet 0 --output adhd_dataset.jsonl

import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd


# ── Cleaning regexes ───────────────────────────────────────────────────────── #
_FANCY_QUOTES = str.maketrans(
    "‘’‚‛“”„‟‹›«»",
    "''''\"\"\"\"<<>>"
)

_RE_CONTROL      = re.compile(r"[\x00-\x1F\x7F]")     # control chars
_RE_REPEATS      = re.compile(r"([-_+*=~]{2,})")      # ----- ___ +++
_RE_BULLETS      = re.compile(r"[•*·]+")              # bullets / dots
_RE_PUNCT_REPEAT = re.compile(r"([!?.,])\1+")         # !!! → !
_RE_BACKSLASH    = re.compile(r"\\+")                 # all back-slashes
_RE_MULTISPACES  = re.compile(r"\s{2,}")              # 2+ spaces


def clean(text: str) -> str:
    """
    Normalise text *without* losing words:
    • strip control chars, repeated separators
    • remove every back-slash
    • convert curly quotes → straight, then any remaining double quote → single
    """
    txt = str(text).translate(_FANCY_QUOTES)
    txt = _RE_BACKSLASH.sub(" ", txt)          # kill back-slashes
    txt = txt.replace('"', "'")                # avoid future escaping
    txt = _RE_CONTROL.sub(" ", txt)
    txt = _RE_REPEATS.sub(" ", txt)
    txt = _RE_BULLETS.sub(" ", txt)
    txt = _RE_PUNCT_REPEAT.sub(r"\1", txt)
    txt = " ".join(txt.strip().split())
    txt = _RE_MULTISPACES.sub(" ", txt)
    return txt


def write_jsonl(df: pd.DataFrame, dest: Path) -> None:
    """Write each row as a chat-formatted JSON object."""
    with dest.open("w", encoding="utf-8") as fh:
        for _, row in df.iterrows():
            fh.write(
                json.dumps(
                    {
                        "conversations": [
                            {"role": "user",      "content": row["prompt"]},
                            {"role": "assistant", "content": row["completion"]}
                        ]
                    },
                    ensure_ascii=False,
                    separators=(",", ":")
                )
                + "\n"
            )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Convert Excel classification data to backslash-free chat JSONL"
    )
    ap.add_argument("excel_path", type=Path, help="Path to Excel workbook")
    ap.add_argument("--sheet", default=None,
                    help="Sheet name (str) or index (int). Default: first")
    ap.add_argument("--output", type=Path, default=Path("dataset.jsonl"),
                    help="Output JSONL file (default: dataset.jsonl)")
    args = ap.parse_args()

    sheet = 0 if args.sheet is None else int(args.sheet) \
        if str(args.sheet).isdigit() else args.sheet

    try:
        df = pd.read_excel(args.excel_path, sheet_name=sheet)
    except Exception as err:
        try:
            print("Available sheets:",
                  pd.ExcelFile(args.excel_path).sheet_names,
                  file=sys.stderr)
        except Exception:
            pass
        sys.exit(f"Failed to read Excel: {err}")

    if not {"Content", "Manual Label"}.issubset(df.columns):
        sys.exit("Missing required columns: Content, Manual Label")

    df = df[["Content", "Manual Label"]].rename(
        columns={"Content": "prompt", "Manual Label": "completion"}
    )
    df["prompt"]     = df["prompt"].apply(clean)
    df["completion"] = df["completion"].apply(lambda x: clean(x).lower())

    before = len(df)
    df = df.dropna()
    if len(df) < before:
        print(f"Dropped {before - len(df)} incomplete rows.", file=sys.stderr)

    write_jsonl(df, args.output)
    print(f"Wrote {len(df):,} records → {args.output.resolve()}")


if __name__ == "__main__":
    main()
