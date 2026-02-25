"""
scraper.py
──────────
Playwright-based MLS website scraper and API discovery tool.

PURPOSE:
  The MLS stats page (mlssoccer.com/stats/players/) is a React SPA.
  This module uses a headless Chromium browser to:
    1. Load the page and accept cookie consent
    2. Intercept all JSON API calls made by the SPA
    3. Return discovered endpoint URLs so httpx can call them directly
    4. Optionally scrape the rendered HTML table as a last-resort fallback

USAGE (standalone discovery run):
    python src/scraper.py

ARCHITECTURE NOTE:
  Playwright is used ONLY to discover or refresh API endpoints.
  Once discovered, all actual data fetching uses httpx (much faster).
  The main pipeline (api_client.py) calls httpx directly using the
  known endpoint URLs confirmed during initial discovery.

CONFIRMED WORKING ENDPOINTS (as of 2026-02-17):
  Season player stats:
    GET https://sportapi.mlssoccer.com/api/stats/players/competition/{comp}/season/{season}/order/goals/desc
    Params: pageSize=50, page=1, clubId={team_id} (optional)
    Headers: Referer: https://www.mlssoccer.com/, Origin: https://www.mlssoccer.com

  Season info (get current season_id):
    GET https://stats-api.mlssoccer.com/competitions/{comp}/seasons

  Club statistics:
    GET https://stats-api.mlssoccer.com/statistics/clubs/competitions/{comp}/seasons/{season}
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

_ROOT         = Path(__file__).parent.parent
_DISCOVERY_CACHE = _ROOT / "data" / "processed" / "discovered_endpoints.json"

MLS_STATS_PAGE = "https://www.mlssoccer.com/stats/players/"

_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Domains to skip when collecting API candidates (analytics, ads, etc.)
_SKIP_DOMAINS = {
    "cookiepro", "doubleclick", "facebook.net", "google",
    "launchdarkly", "equalweb", "chartbeat", "zip.co",
    "gofevo", "snowplow", "jsdelivr", "srcspot",
}


def _should_capture(url: str) -> bool:
    """Return True if this URL looks like a data API call worth recording."""
    return (
        "json" in url.lower() or
        any(kw in url for kw in ("api", "stats", "sport", "opta", "data", "player"))
    ) and not any(skip in url for skip in _SKIP_DOMAINS)


class MLSScraper:
    """
    Headless Chromium wrapper for MLS website discovery.

    The primary method is `discover_api_calls()` which loads the MLS
    stats page, fires off a consent click, waits for React to fetch data,
    and returns every JSON API URL it intercepted.
    """

    def __init__(self, headless: bool = True, wait_seconds: int = 10) -> None:
        self.headless     = headless
        self.wait_seconds = wait_seconds

    def discover_api_calls(self) -> list[dict[str, str]]:
        """
        Load the MLS stats page and capture all API calls made by the SPA.

        Returns a list of dicts:
            [{"url": "https://...", "status": "200", "content_type": "..."}, ...]

        Results are also saved to /data/processed/discovered_endpoints.json.
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            print("[Scraper] Playwright not installed. Run: pip install playwright && playwright install chromium")
            return []

        captured: list[dict[str, str]] = []

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self.headless)
            ctx     = browser.new_context(user_agent=_BROWSER_UA)
            page    = ctx.new_page()

            def _on_response(resp) -> None:
                url = resp.url
                if not _should_capture(url):
                    return
                ct = resp.headers.get("content-type", "")
                captured.append({
                    "url":          url,
                    "status":       str(resp.status),
                    "content_type": ct[:80],
                })

            page.on("response", _on_response)

            print(f"[Scraper] Loading {MLS_STATS_PAGE} …")
            page.goto(MLS_STATS_PAGE, timeout=45_000, wait_until="domcontentloaded")
            time.sleep(3)

            # Accept OneTrust cookie consent — required for stats JS to execute
            for selector in ("#onetrust-accept-btn-handler", "text=Accept All", "text=Accept Cookies"):
                try:
                    page.click(selector, timeout=4_000)
                    print("[Scraper] Cookie consent accepted.")
                    time.sleep(1)
                    break
                except Exception:
                    pass

            # Scroll to trigger lazy-load sections
            page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
            time.sleep(self.wait_seconds)

            browser.close()

        print(f"[Scraper] Captured {len(captured)} API-like responses.")

        # Persist results
        _ROOT.joinpath("data", "processed").mkdir(parents=True, exist_ok=True)
        with open(_DISCOVERY_CACHE, "w") as f:
            json.dump({"captured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"), "endpoints": captured}, f, indent=2)
        print(f"[Scraper] Saved to {_DISCOVERY_CACHE}")

        return captured

    def scrape_stats_table(self) -> list[dict[str, Any]]:
        """
        Last-resort fallback: render the page and parse the visible stats table.

        Returns a list of row dicts keyed by column header.
        Only useful if the API approach completely fails.
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            return []

        rows: list[dict[str, Any]] = []

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self.headless)
            ctx     = browser.new_context(user_agent=_BROWSER_UA)
            page    = ctx.new_page()

            page.goto(MLS_STATS_PAGE, timeout=45_000, wait_until="domcontentloaded")
            time.sleep(3)

            for selector in ("#onetrust-accept-btn-handler", "text=Accept All"):
                try:
                    page.click(selector, timeout=4_000)
                    time.sleep(2)
                    break
                except Exception:
                    pass

            time.sleep(8)

            # Find table headers and rows
            headers = page.eval_on_selector_all(
                "table thead th",
                "els => els.map(e => e.innerText.trim())"
            )

            table_rows = page.eval_on_selector_all(
                "table tbody tr",
                "rows => rows.map(r => Array.from(r.querySelectorAll('td')).map(td => td.innerText.trim()))"
            )

            for raw_row in table_rows:
                if len(raw_row) == len(headers):
                    rows.append(dict(zip(headers, raw_row)))

            browser.close()

        print(f"[Scraper] Scraped {len(rows)} rows from HTML table.")
        return rows


# ── Standalone run ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper  = MLSScraper(headless=True, wait_seconds=10)
    captured = scraper.discover_api_calls()

    print("\n╔══════════════════════════════════════════════════════════════╗")
    print("║  MLS API Discovery Results                                   ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    for item in captured:
        status = item["status"]
        url    = item["url"][:90]
        print(f"║  [{status}]  {url:<56}║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"\nFull results saved to: {_DISCOVERY_CACHE}")
    print("Use these URLs to update src/config.py if endpoints change.")
