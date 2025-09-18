#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PDF Extractor: text + tables using pdfplumber
---------------------------------------------

Requirement : 
    a. pdfplumber  ->   https://pdfplumber.github.io/
        pip install pdfplumber
  
Features:
- Accepts local PDF path OR a URL to download first
- Extracts full text per-page to a single TXT file (+ optional JSONL per-page)
- Extracts tables (all pages) to per-table CSVs and a merged CSV (columns deduped)
- Saves simple metadata (page count, file size, creation/mod dates if present)
- Robust error handling and CLI options

Usage examples:
  # local file
  python document_parsing/pdf_extract.py --in ./samples/sample.pdf --outdir out/sample_pdf

  # download from URL
  python document_parsing/pdf_extract.py --url https://arxiv.org/pdf/1706.03762.pdf --outdir out/attention

  # tweak table extraction sensitivity
  python document_parsing/pdf_extract.py --in sample.pdf --outdir out/sample --edge-min 3 --text-x-tol 2 --text-y-tol 2
"""

from __future__ import annotations
import argparse, json, sys, time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests
import pdfplumber
import pandas as pd


# --------------------------- Data structures ---------------------------

@dataclass
class ExtractStats:
    source: str
    saved_dir: str
    pages: int
    tables_found: int
    text_bytes: int
    elapsed_seconds: float
    metadata: Dict[str, Any]


# --------------------------- Helpers ---------------------------

def safe_filename(base: str) -> str:
    return "".join(c if c.isalnum() or c in "._- " else "_" for c in base)

def download_pdf(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)
    return dest

def get_pdf_metadata(pdf: pdfplumber.PDF) -> Dict[str, Any]:
    md = pdf.metadata or {}
    clean = {k: (str(v) if v is not None else None) for k, v in md.items()}
    return {
        "Title": clean.get("Title"),
        "Author": clean.get("Author"),
        "Creator": clean.get("Creator"),
        "Producer": clean.get("Producer"),
        "CreationDate": clean.get("CreationDate"),
        "ModDate": clean.get("ModDate"),
        "Subject": clean.get("Subject"),
        "Keywords": clean.get("Keywords"),
    }


# --------------------------- Text extraction ---------------------------

def extract_text(pdf: pdfplumber.PDF, out_txt: Path, out_jsonl: Optional[Path] = None) -> int:
    out_txt.parent.mkdir(parents=True, exist_ok=True)
    text_bytes = 0
    # Write TXT and optional JSONL (one record per page)
    with open(out_txt, "w", encoding="utf-8") as f_txt:
        for i, page in enumerate(pdf.pages, start=1):
            page_text = page.extract_text() or ""
            header = f"\n\n===== [Page {i}] =====\n\n"
            f_txt.write(header + page_text)
            text_bytes += len(page_text.encode("utf-8"))
            if out_jsonl:
                out_jsonl.parent.mkdir(parents=True, exist_ok=True)
                with open(out_jsonl, "a", encoding="utf-8") as f_jsonl:
                    f_jsonl.write(json.dumps({"page": i, "text": page_text}, ensure_ascii=False) + "\n")
    return text_bytes


# --------------------------- Table extraction (deduped headers) ---------------------------

def _make_unique(cols):
    """Ensure column labels are unique: col, col_1, col_2, ..."""
    seen = {}
    out = []
    for c in cols:
        key = "" if c is None else str(c).strip()
        if key == "":
            key = "col"
        if key not in seen:
            seen[key] = 0
            out.append(key)
        else:
            seen[key] += 1
            out.append(f"{key}_{seen[key]}")
    return out

def extract_tables(
    pdf: pdfplumber.PDF,
    tables_dir: Path,
    merged_csv: Path,
    table_settings: Dict[str, Any],
) -> int:
    tables_dir.mkdir(parents=True, exist_ok=True)
    all_tables: List[pd.DataFrame] = []
    table_idx = 0

    for page_i, page in enumerate(pdf.pages, start=1):
        # Try with custom settings; if a page fails, fall back to defaults
        try:
            tlist = page.extract_tables(table_settings=table_settings) or []
        except Exception:
            tlist = page.extract_tables() or []

        for tbl in tlist:
            # Build dataframe from raw rows (ragged rows are okay; pandas fills NaN)
            df = pd.DataFrame(tbl)

            # If first row looks like headers, use it; then dedupe headers
            if df.shape[0] > 1 and any(x is not None and str(x).strip() != "" for x in df.iloc[0].tolist()):
                headers = _make_unique(df.iloc[0].tolist())
                df = df.iloc[1:].reset_index(drop=True)
                df.columns = headers
            else:
                # No header row → create generic unique headers
                df.columns = _make_unique([f"col_{i}" for i in range(df.shape[1])])

            table_idx += 1
            csv_path = tables_dir / f"table_p{page_i}_{table_idx}.csv"
            df.to_csv(csv_path, index=False)
            all_tables.append(df)

    if all_tables:
        # Align by union of columns; columns are already unique per-table
        merged = pd.concat(all_tables, axis=0, ignore_index=True, sort=False)
        merged.to_csv(merged_csv, index=False)

    return table_idx


# --------------------------- CLI ---------------------------

def main():
    parser = argparse.ArgumentParser(description="Extract text and tables from PDF into TXT/CSV/JSON.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--in", dest="infile", type=str, help="Path to a local PDF.")
    group.add_argument("--url", dest="url", type=str, help="URL to a PDF to download first.")
    parser.add_argument("--outdir", type=str, required=True, help="Output directory.")
    parser.add_argument("--jsonl", action="store_true", help="Also save per-page text as JSONL.")
    # Table tuning (pdfplumber settings)
    parser.add_argument("--edge-min", type=int, default=2, help="Min words to detect table edges.")
    parser.add_argument("--text-x-tol", type=float, default=1.0, help="Tolerance for merging text boxes horizontally.")
    parser.add_argument("--text-y-tol", type=float, default=1.0, help="Tolerance for merging text boxes vertically.")
    args = parser.parse_args()

    t0 = time.time()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # 1) Resolve input source
    if args.infile:
        pdf_path = Path(args.infile)
        if not pdf_path.exists():
            print(f"❌ File not found: {pdf_path}", file=sys.stderr)
            sys.exit(2)
        source_label = str(pdf_path)
    else:
        pdf_name = safe_filename(Path(args.url).name or "downloaded.pdf")
        pdf_path = outdir / pdf_name
        try:
            download_pdf(args.url, pdf_path)
            source_label = args.url
        except requests.RequestException as e:
            print(f"❌ Download error: {e}", file=sys.stderr)
            sys.exit(3)

    # 2) Open and extract
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            meta = get_pdf_metadata(pdf)
            pages = len(pdf.pages)

            text_txt = outdir / "text.txt"
            text_jsonl = outdir / "text.jsonl" if args.jsonl else None
            text_bytes = extract_text(pdf, text_txt, text_jsonl)

            tables_dir = outdir / "tables"
            merged_csv = outdir / "tables_merged.csv"
            table_settings = {
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "min_words_vertical": args.edge_min,
                "min_words_horizontal": args.edge_min,
                "text_x_tolerance": args.text_x_tol,
                "text_y_tolerance": args.text_y_tol,
            }
            tables_found = extract_tables(pdf, tables_dir, merged_csv, table_settings)

    except Exception as e:
        print(f"❌ PDF processing error: {e}", file=sys.stderr)
        sys.exit(4)

    # 3) Save summary
    stats = ExtractStats(
        source=source_label,
        saved_dir=str(outdir),
        pages=pages,
        tables_found=tables_found,
        text_bytes=text_bytes,
        elapsed_seconds=round(time.time() - t0, 2),
        metadata=meta,
    )
    with open(outdir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(asdict(stats), f, ensure_ascii=False, indent=2)

    print(f"✅ Done. Pages={pages} | Tables={tables_found} | TextBytes={text_bytes} | Out={outdir}")

if __name__ == "__main__":
    main()
