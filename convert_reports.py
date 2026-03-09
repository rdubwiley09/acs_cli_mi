#!/usr/bin/env python3
"""Convert HTML reports in the reports/ directory to PDF using Playwright."""

import glob
import os
import sys
from pathlib import Path
from playwright.sync_api import sync_playwright


REPORTS_DIR = Path(__file__).parent / "reports"


def convert_html_to_pdf(html_path: Path, pdf_path: Path) -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})

        page.goto(html_path.as_uri(), wait_until="networkidle")

        # Wait for JS-rendered content, fonts, and map tiles to settle
        page.wait_for_timeout(3000)

        # Force all animated elements visible and refresh map layouts
        page.evaluate("""() => {
            document.querySelectorAll('.anim').forEach(el => el.classList.add('visible'));
            if (typeof ALL_MAPS !== 'undefined') {
                ALL_MAPS.forEach(m => m.invalidateSize());
            }
        }""")
        page.wait_for_timeout(1000)

        # Inject print-friendly tweaks so the PDF looks clean
        page.add_style_tag(content="""
            @media print {
                /* Prevent clipping and ensure backgrounds render */
                body { -webkit-print-color-adjust: exact; print-color-adjust: exact; }

                /* Hide interactive-only elements */
                .table-controls, .leaflet-control-zoom, .leaflet-control-attribution { display: none !important; }

                /* Page-break rules: only avoid breaks inside small self-contained
                   elements. Sections and the table MUST span pages freely. */
                .card, .profile-card, .analysis-card, .callout, .domain-card,
                .map-card, thead { break-inside: avoid; }
                .section, .table-wrap { break-inside: auto !important; }
                thead { display: table-header-group; }
                tr { page-break-inside: avoid; }

                /* Tighten spacing for print */
                .section { padding: 24px 0; }
                .hero { padding: 28px 0 36px; }
            }
        """)

        page.pdf(
            path=str(pdf_path),
            format="A4",
            print_background=True,
            margin={"top": "16mm", "right": "12mm", "bottom": "16mm", "left": "12mm"},
            scale=0.85,
        )

        browser.close()
        print(f"  -> {pdf_path}")


def main() -> None:
    html_files = sorted(REPORTS_DIR.glob("*.html"))
    if not html_files:
        print("No HTML files found in reports/")
        sys.exit(1)

    print(f"Found {len(html_files)} HTML report(s) to convert:\n")

    for html_path in html_files:
        pdf_path = html_path.with_suffix(".pdf")
        print(f"Converting {html_path.name}...")
        convert_html_to_pdf(html_path, pdf_path)

    print("\nDone.")


if __name__ == "__main__":
    main()
