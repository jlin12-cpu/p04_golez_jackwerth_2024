"""
Generate self-contained HTML pages for figures and tables used in the chartbook site.

This script creates lightweight HTML wrappers around the existing PNG figures
and CSV tables produced by the replication pipeline. To avoid broken image-path
issues inside the chartbook website, figures are embedded directly into the HTML
as base64 data URIs. This makes each generated HTML page self-contained.

Inputs:
    output/figure1/figure1.png
    output/figure1/figure1_summary.csv
    output/figure1_extension/figure1_extension.png
    output/figure1_extension/figure1_extension_summary.csv
    output/figure1_summary_stats/figure1_implied_zero_spread.png
    output/figure1_summary_stats/figure1_summary_stats_table.csv
    output/figure2/figure2.png
    output/figure2/figure2_terminal_comparison.csv
    output/figure2_extended/figure2_extended.png
    output/figure2_extended/figure2_extended_terminal.csv
    output/figure2_extended_winsorized/figure2_extended_winsorized.png
    output/figure2_extended_winsorized/figure2_extended_winsorized_terminal.csv
    output/figure3/figure3.png
    output/figure3/figure3_series.csv
    output/figure3_extended/figure3_extended.png
    output/figure3_extended/figure3_extended_series.csv
    output/table1.csv
    output/table1_extended.csv

Outputs:
    output/site_html/figure1.html
    output/site_html/figure1_extension.html
    output/site_html/figure1_summary_stats.html
    output/site_html/figure2.html
    output/site_html/figure2_extension.html
    output/site_html/figure2_extension_winsorized.html
    output/site_html/figure3.html
    output/site_html/figure3_extension.html
    output/site_html/table1.html
    output/site_html/table1_extended.html
"""

from pathlib import Path
import base64
import html
import mimetypes
import pandas as pd

OUT = Path("output")
SITE_DIR = OUT / "site_html"
SITE_DIR.mkdir(parents=True, exist_ok=True)


def style_block() -> str:
    """Shared CSS styling for all generated HTML pages."""
    return """
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
            max-width: 1100px;
            margin: 40px auto;
            padding: 0 20px;
            line-height: 1.6;
            color: #222;
            background: #fff;
        }
        h1, h2, h3 {
            color: #111;
        }
        h1 {
            margin-bottom: 0.4rem;
            line-height: 1.2;
        }
        .meta {
            color: #666;
            margin-bottom: 1.2rem;
        }
        .caption {
            background: #f7f7f7;
            border-left: 4px solid #cccccc;
            padding: 12px 16px;
            margin: 1rem 0 1.5rem 0;
        }
        .img-wrap {
            margin: 1.5rem 0;
            text-align: center;
        }
        img {
            max-width: 100%;
            height: auto;
            border: 1px solid #ddd;
            background: white;
        }
        table.dataframe {
            border-collapse: collapse;
            width: 100%;
            margin: 1rem 0 2rem 0;
            font-size: 0.95rem;
        }
        table.dataframe th,
        table.dataframe td {
            border: 1px solid #d9d9d9;
            padding: 8px 10px;
            text-align: right;
        }
        table.dataframe th:first-child,
        table.dataframe td:first-child {
            text-align: left;
        }
        table.dataframe thead th {
            background-color: #f0f0f0;
        }
        .note {
            color: #444;
            font-size: 0.95rem;
        }
        .missing {
            padding: 1rem;
            background: #fff3cd;
            border: 1px solid #ffe69c;
            color: #7a5c00;
            margin: 1rem 0;
        }
        .small {
            color: #777;
            font-size: 0.9rem;
        }
    </style>
    """


def image_to_data_uri(img_path: Path) -> str | None:
    """
    Convert an image file to a base64 data URI.

    Returns None if the file does not exist.
    """
    if not img_path.exists():
        return None

    mime_type, _ = mimetypes.guess_type(str(img_path))
    if mime_type is None:
        mime_type = "image/png"

    img_bytes = img_path.read_bytes()
    encoded = base64.b64encode(img_bytes).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def build_page(
    title: str,
    subtitle: str,
    caption: str,
    img_path: Path | None = None,
    tables: list[tuple[str, pd.DataFrame]] | None = None,
    notes: list[str] | None = None,
) -> str:
    """
    Build a complete self-contained HTML page.

    Parameters
    ----------
    title : str
        Page title.
    subtitle : str
        Subtitle shown below the title.
    caption : str
        Description shown in the caption block.
    img_path : Path | None
        Path to the PNG figure. The image is embedded directly as a base64 data URI.
    tables : list[tuple[str, pd.DataFrame]] | None
        List of (section_title, DataFrame) pairs to render as HTML tables.
    notes : list[str] | None
        List of notes shown at the bottom of the page.
    """
    tables = tables or []
    notes = notes or []

    parts = [
        "<!DOCTYPE html>",
        '<html lang="en">',
        "<head>",
        '  <meta charset="utf-8">',
        f"  <title>{html.escape(title)}</title>",
        '  <meta name="viewport" content="width=device-width, initial-scale=1">',
        style_block(),
        "</head>",
        "<body>",
        f"  <h1>{html.escape(title)}</h1>",
        f'  <div class="meta">{html.escape(subtitle)}</div>',
        f'  <div class="caption">{html.escape(caption)}</div>',
    ]

    if img_path is not None:
        data_uri = image_to_data_uri(img_path)
        if data_uri is not None:
            parts.extend([
                '  <div class="img-wrap">',
                f'    <img src="{data_uri}" alt="{html.escape(title)}">',
                "  </div>",
            ])
        else:
            parts.append(
                f'  <div class="missing">Missing image file: {html.escape(str(img_path))}</div>'
            )

    for section_title, df in tables:
        parts.append(f"  <h2>{html.escape(section_title)}</h2>")
        parts.append(
            df.to_html(index=False, classes="dataframe", border=0, escape=False)
        )

    if notes:
        parts.append("  <h2>Notes</h2>")
        for note in notes:
            parts.append(f'  <p class="note">{html.escape(note)}</p>')

    parts.extend([
        "</body>",
        "</html>",
    ])
    return "\n".join(parts)


def safe_read_csv(path: Path, **kwargs) -> pd.DataFrame | None:
    """Read a CSV file if it exists; otherwise return None."""
    if not path.exists():
        return None
    return pd.read_csv(path, **kwargs)


def write_html(filename: str, html_text: str) -> None:
    """Write HTML text to output/site_html/<filename>."""
    out_path = SITE_DIR / filename
    out_path.write_text(html_text, encoding="utf-8")
    print(f"Saved {out_path}")


def generate_figure1_page() -> None:
    """Figure 1: 12-month interest rates replication."""
    img = OUT / "figure1" / "figure1.png"
    df = safe_read_csv(OUT / "figure1" / "figure1_summary.csv")
    tables = [("Summary Statistics", df)] if df is not None else []

    write_html("figure1.html", build_page(
        title="Figure 1: 12-Month Interest Rates",
        subtitle="Replication sample: January 1996 to December 2022",
        caption=(
            "Replicated Figure 1 comparing the option-implied 1-year interest rate, "
            "the OptionMetrics zero-curve 1-year rate, and the 1-year constant maturity "
            "Treasury rate from FRED."
        ),
        img_path=img,
        tables=tables,
        notes=[
            "The option-implied rate is estimated from SPX put-call parity pairs.",
            "The figure is part of the fully automated replication pipeline.",
        ],
    ))


def generate_figure1_extension_page() -> None:
    """Figure 1 extension through 2024."""
    img = OUT / "figure1_extension" / "figure1_extension.png"
    df = safe_read_csv(OUT / "figure1_extension" / "figure1_extension_summary.csv")
    tables = [("Extension Summary Statistics", df)] if df is not None else []

    write_html("figure1_extension.html", build_page(
        title="Figure 1 Extension",
        subtitle="Extended sample through the latest available month",
        caption=(
            "Extended-sample version of Figure 1, showing how the relationship between "
            "the option-implied rate and the zero-curve benchmark changes after the end "
            "of the original paper sample."
        ),
        img_path=img,
        tables=tables,
        notes=[
            "The implied-minus-zero spread turns negative on average in 2023-2024.",
            "These results are generated automatically from the updated pipeline inputs.",
        ],
    ))


def generate_figure1_summary_stats_page() -> None:
    """Figure 1 descriptive statistics and implied-zero spread chart."""
    img = OUT / "figure1_summary_stats" / "figure1_implied_zero_spread.png"
    df = safe_read_csv(OUT / "figure1_summary_stats" / "figure1_summary_stats_table.csv")
    tables = [("Descriptive Statistics", df)] if df is not None else []

    write_html("figure1_summary_stats.html", build_page(
        title="Figure 1 Summary Statistics and Spread Diagnostics",
        subtitle="Supporting analysis for the Figure 1 replication",
        caption=(
            "Descriptive statistics for the three 12-month interest rate series used in "
            "Figure 1, together with a time series of the implied-minus-zero spread in "
            "basis points."
        ),
        img_path=img,
        tables=tables,
        notes=[
            "Statistics are reported for the full sample and two sub-periods.",
            "The spread is measured in basis points.",
        ],
    ))


def generate_figure2_page() -> None:
    """Figure 2: cumulative returns replication."""
    img = OUT / "figure2" / "figure2.png"
    df = safe_read_csv(OUT / "figure2" / "figure2_terminal_comparison.csv")
    tables = [("Terminal Value Comparison", df)] if df is not None else []

    write_html("figure2.html", build_page(
        title="Figure 2: Cumulative Returns",
        subtitle="Replication sample: January 1996 to December 2022",
        caption=(
            "Cumulative wealth indices for a $1 investment in the dividend strip and the "
            "market under raw returns, excess returns over the risk-free rate, and excess "
            "returns over duration-matched Treasury returns."
        ),
        img_path=img,
        tables=tables,
        notes=[
            "Market-side terminal values are reasonably close to the published figure.",
            "Strip-side terminal values are higher due to the automated CRSP/OptionMetrics pipeline.",
        ],
    ))


def generate_figure2_extension_page() -> None:
    """Figure 2 extension through 2024 (raw)."""
    img = OUT / "figure2_extended" / "figure2_extended.png"
    df = safe_read_csv(OUT / "figure2_extended" / "figure2_extended_terminal.csv")
    tables = [("Extended Terminal Values", df)] if df is not None else []

    write_html("figure2_extension.html", build_page(
        title="Figure 2 Extension",
        subtitle="Extended sample through the latest available month",
        caption=(
            "Raw extended-sample version of Figure 2. The post-2022 strip series becomes "
            "explosive in cumulative-return form due to extreme monthly strip returns."
        ),
        img_path=img,
        tables=tables,
        notes=[
            "This raw extension is useful diagnostically but should be interpreted with caution.",
            "See the winsorized version for a more interpretable robustness check.",
        ],
    ))


def generate_figure2_extension_winsorized_page() -> None:
    """Figure 2 extension through 2024 (winsorized)."""
    img = OUT / "figure2_extended_winsorized" / "figure2_extended_winsorized.png"
    df = safe_read_csv(
        OUT / "figure2_extended_winsorized" / "figure2_extended_winsorized_terminal.csv"
    )
    tables = [("Winsorized Extended Terminal Values", df)] if df is not None else []

    write_html("figure2_extension_winsorized.html", build_page(
        title="Figure 2 Extension (Winsorized)",
        subtitle="Extended sample with post-2022 strip-return clipping at ±0.50",
        caption=(
            "Robustness version of the extended Figure 2 in which post-2022 strip log "
            "returns are winsorized at ±0.50 to reduce the influence of extreme "
            "observations likely driven by put-call parity violations."
        ),
        img_path=img,
        tables=tables,
        notes=[
            "The market series is unchanged by this adjustment.",
            "The winsorized figure is more interpretable than the raw extension.",
        ],
    ))


def generate_figure3_page() -> None:
    """Figure 3: volatility across holding periods replication."""
    img = OUT / "figure3" / "figure3.png"
    df = safe_read_csv(OUT / "figure3" / "figure3_series.csv")
    tables = [("Annualized Volatility by Holding Period (%)", df)] if df is not None else []

    write_html("figure3.html", build_page(
        title="Figure 3: Volatility Across Holding Periods",
        subtitle="Replication sample: January 1996 to December 2022",
        caption=(
            "Annualized standard deviations of strip and market excess returns across "
            "holding periods from 1 to 36 months. The sharp decline in strip volatility "
            "with horizon is consistent with measurement error in option-derived strip prices."
        ),
        img_path=img,
        tables=tables,
        notes=[
            "Market-side volatility remains broadly flat across holding periods.",
            "Strip volatility converges below market volatility by the 36-month horizon.",
        ],
    ))


def generate_figure3_extension_page() -> None:
    """Figure 3 extension through 2024."""
    img = OUT / "figure3_extended" / "figure3_extended.png"
    df = safe_read_csv(OUT / "figure3_extended" / "figure3_extended_series.csv")
    tables = [("Extended Annualized Volatility by Holding Period (%)", df)] if df is not None else []

    write_html("figure3_extension.html", build_page(
        title="Figure 3 Extension",
        subtitle="Extended sample through the latest available month",
        caption=(
            "Extended-sample version of Figure 3. Strip-side volatility is materially "
            "elevated at all holding periods and does not converge below market volatility "
            "even at the 36-month horizon, indicating persistent structural instability in "
            "post-2022 strip price estimates."
        ),
        img_path=img,
        tables=tables,
        notes=[
            "Market-side volatility remains broadly stable relative to the paper sample.",
            "The elevated strip volatility curve confirms that late-sample instability is structural rather than isolated.",
        ],
    ))


def generate_table1_page() -> None:
    """Table 1: monthly return summary statistics replication."""
    df = safe_read_csv(OUT / "table1.csv")
    tables = [("Replicated Table 1", df)] if df is not None else []

    write_html("table1.html", build_page(
        title="Table 1: Monthly Return Summary Statistics",
        subtitle="Replication sample: January 1996 to December 2022",
        caption=(
            "Replicated Table 1 reporting annualized means, standard deviations, Sharpe "
            "ratios, AR(1) autocorrelations, and observation counts for market and dividend "
            "strip returns under raw and excess-return definitions."
        ),
        tables=tables,
        notes=[
            "Market moments are close to the published paper.",
            "Strip moments are qualitatively similar but quantitatively more extreme in the automated pipeline.",
        ],
    ))


def generate_table1_extended_page() -> None:
    """Table 1 extension through 2024."""
    df = safe_read_csv(OUT / "table1_extended.csv")
    tables = [("Extended Table 1", df)] if df is not None else []

    write_html("table1_extended.html", build_page(
        title="Table 1 Extension",
        subtitle="Extended sample through the latest available month",
        caption=(
            "Extended-sample version of Table 1 showing that market-side moments remain "
            "broadly stable while strip-side moments become much more extreme in 2023-2024."
        ),
        tables=tables,
        notes=[
            "The post-2022 period has a large effect on strip mean and volatility.",
            "These extension results are best interpreted as exploratory diagnostics.",
        ],
    ))


if __name__ == "__main__":
    print("=" * 60)
    print("GENERATING HTML PAGES FOR CHARTBOOK / WEBSITE")
    print("=" * 60)

    generate_figure1_page()
    generate_figure1_extension_page()
    generate_figure1_summary_stats_page()
    generate_figure2_page()
    generate_figure2_extension_page()
    generate_figure2_extension_winsorized_page()
    generate_figure3_page()
    generate_figure3_extension_page()
    generate_table1_page()
    generate_table1_extended_page()

    print("\nDone!")
    print(f"HTML files saved to: {SITE_DIR}")