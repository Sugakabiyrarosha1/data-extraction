#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape quotes from https://quotes.toscrape.com (a site built for scraping demos).
Outputs a clean CSV with columns: quote, author, tags, author_url, source_url.

Usage:
  python web_scraping/scrape_quotes.py --max-pages 5 --out data/quotes.csv --delay 0.7
"""

from __future__ import annotations
import csv, time, sys, argparse
from pathlib import Path
from typing import Iterator, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://quotes.toscrape.com"

def make_session() -> requests.Session:
    # creating a session with retry logic to handle transient failures
    s = requests.Session()
    s.headers.update({
        "User-Agent": "DataExtractionShowcase/1.0 (+https://github.com/<your-username>)"
    })
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def parse_quote_block(block) -> Dict[str, str]:
    # parsing a single quote block to extract fields
    text = block.select_one("span.text")
    author = block.select_one("small.author")
    tags = [t.get_text(strip=True) for t in block.select("div.tags a.tag")]
    author_link = block.select_one("span a[href^='/author/']")
    return {
        "quote": (text.get_text(strip=True) if text else ""),
        "author": (author.get_text(strip=True) if author else ""),
        "tags": ",".join(tags),
        "author_url": (BASE_URL + author_link["href"] if author_link else ""),
    }

def iter_pages(session: requests.Session, max_pages: Optional[int], delay: float) -> Iterator[str]:
    # iterating pages following "Next" links
    page_url = BASE_URL
    page_count = 0
    while page_url:
        page_count += 1
        if max_pages and page_count > max_pages:
            break
        resp = session.get(page_url, timeout=15)
        resp.raise_for_status()
        yield resp.text
        soup = BeautifulSoup(resp.text, "lxml")
        next_link = soup.select_one("li.next > a")
        page_url = BASE_URL + next_link["href"] if next_link else None
        if page_url and delay > 0:
            time.sleep(delay)

def scrape_quotes(max_pages: Optional[int], delay: float) -> List[Dict[str, str]]:
    # coordinating pagination and parsing
    session = make_session()
    results: List[Dict[str, str]] = []
    page_index = 0

    for html in iter_pages(session, max_pages=max_pages, delay=delay):
        page_index += 1
        soup = BeautifulSoup(html, "lxml")
        blocks = soup.select("div.quote")
        for b in blocks:
            row = parse_quote_block(b)
            row["source_url"] = f"{BASE_URL}/page/{page_index}/" if page_index > 1 else BASE_URL
            results.append(row)
    return results

def write_csv(rows: List[Dict[str, str]], out_path: Path) -> None:
    # writing the results as csv
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["quote", "author", "tags", "author_url", "source_url"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def main():
    parser = argparse.ArgumentParser(description="Scrape quotes.toscrape.com into CSV.")
    parser.add_argument("--max-pages", type=int, default=0, help="Max pages to scrape (0 = all).")
    parser.add_argument("--out", type=str, default="data/quotes.csv", help="Output CSV path.")
    parser.add_argument("--delay", type=float, default=0.6, help="Delay (seconds) between page requests.")
    args = parser.parse_args()

    try:
        rows = scrape_quotes(max_pages=(args.max_pages or None), delay=args.delay)
        write_csv(rows, Path(args.out))
        print(f"✅ Scraped {len(rows)} rows → {args.out}")
    except requests.HTTPError as e:
        print(f"❌ HTTP error: {e}", file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as e:
        print(f"❌ Network error: {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"❌ Unexpected error: {e}", file=sys.stderr)
        sys.exit(3)

if __name__ == "__main__":
    main()