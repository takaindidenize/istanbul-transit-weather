"""Estimate how weather changes hourly ridership, controlling for time of week and season.

For each transport mode, an OLS regression on the hourly mart:

    log(passages) ~ rain intensity + snow + temperature band + wind
                    + day-of-week × hour FE + public holiday × hour FE + year-month FE

The raw rainy-vs-dry comparison in ``mart_weather_effect`` is confounded by season (it rains
more in winter, when schools are open); the year-month fixed effects remove that. Standard
errors are clustered by date because weather and ridership are correlated within a day.

Writes ``docs/analysis/results.md`` and charts under ``docs/img/``.

Usage: python analysis/weather_effect.py   (after `dbt build`)
"""

from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from transit_weather import config  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
OUT_MD = ROOT / "docs" / "analysis" / "results.md"
IMG_DIR = ROOT / "docs" / "img"

MODES = ("road", "rail", "sea")
MODE_LABELS = {
    "road": "Road (bus, metrobus)",
    "rail": "Rail (metro, tram, Marmaray)",
    "sea": "Sea (ferries)",
}
# Service hours only: overnight counts are tiny and dominated by noise.
FIRST_HOUR, LAST_HOUR = 6, 23

RAIN_BINS = [
    ("rain_light", "Light rain (0.1–1 mm/h)", 0.1, 1.0),
    ("rain_moderate", "Moderate rain (1–4 mm/h)", 1.0, 4.0),
    ("rain_heavy", "Heavy rain (≥4 mm/h)", 4.0, np.inf),
]
TEMP_BANDS = [("<5", "Below 5 °C"), ("5-15", "5–15 °C"), (">=25", "25 °C and above")]  # vs 15–25
WEATHER_TERMS = [
    *[(name, label) for name, label, _, _ in RAIN_BINS],
    ("snow", "Snowfall"),
    *[(f"temp_{band}", label) for band, label in TEMP_BANDS],
    ("windy", "Strong wind (≥30 km/h)"),
]


def load(warehouse: Path) -> pd.DataFrame:
    with duckdb.connect(str(warehouse), read_only=True) as con:
        return con.execute(
            """
            SELECT observed_at, date, hour, day_of_week, is_holiday, transport_mode, passages,
                   precipitation, snowfall, wind_speed_10m, temp_band
            FROM fct_hourly_ridership_weather
            WHERE passages > 0
              AND precipitation IS NOT NULL
              AND hour BETWEEN ? AND ?
            """,
            [FIRST_HOUR, LAST_HOUR],
        ).df()


def design(df: pd.DataFrame) -> tuple[np.ndarray, list[str], np.ndarray, np.ndarray]:
    """Design matrix (weather terms first), names, log outcome and date clusters."""
    snow = df["snowfall"] > 0
    terms = {}
    for name, _, low, high in RAIN_BINS:
        terms[name] = ~snow & (df["precipitation"] >= low) & (df["precipitation"] < high)
    terms["snow"] = snow
    for band, _ in TEMP_BANDS:
        terms[f"temp_{band}"] = df["temp_band"] == band
    terms["windy"] = df["wind_speed_10m"] >= 30
    weather = pd.DataFrame(terms).astype(float)

    week_hour = df["day_of_week"].astype(str) + "_" + df["hour"].astype(str)
    holiday_hour = np.where(df["is_holiday"], "hol_" + df["hour"].astype(str), "none")
    month = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
    fixed = pd.concat(
        [
            pd.get_dummies(week_hour, prefix="wh", dtype=float),  # full set, acts as intercept
            pd.get_dummies(pd.Series(holiday_hour), prefix="hol", dtype=float).drop(
                columns="hol_none", errors="ignore"
            ),
            pd.get_dummies(month, prefix="m", drop_first=True, dtype=float),
        ],
        axis=1,
    )
    fixed.index = weather.index
    x = pd.concat([weather, fixed], axis=1)
    x = x.loc[:, x.std() > 0] if len(x) > 1 else x
    names = list(x.columns)
    return x.to_numpy(), names, np.log(df["passages"].to_numpy(float)), df["date"].to_numpy()


def ols_clustered(x: np.ndarray, y: np.ndarray, clusters: np.ndarray):
    """OLS coefficients with cluster-robust (CR1) standard errors."""
    xtx_inv = np.linalg.pinv(x.T @ x)
    beta = xtx_inv @ x.T @ y
    resid = y - x @ beta
    meat = np.zeros_like(xtx_inv)
    codes, groups = pd.factorize(clusters)
    for g in range(len(groups)):
        idx = codes == g
        score = x[idx].T @ resid[idx]
        meat += np.outer(score, score)
    n, k, g = len(y), x.shape[1], len(groups)
    scale = (g / (g - 1)) * ((n - 1) / (n - k))
    se = np.sqrt(np.clip(np.diag(scale * xtx_inv @ meat @ xtx_inv), 0, None))
    r2 = 1 - resid.var() / y.var()
    return beta, se, r2


def estimate(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    rows, fit = [], {}
    for mode in MODES:
        sub = df[df["transport_mode"] == mode].reset_index(drop=True)
        if sub.empty:
            continue
        x, names, y, clusters = design(sub)
        beta, se, r2 = ols_clustered(x, y, clusters)
        fit[mode] = {"n_hours": len(sub), "n_days": sub["date"].nunique(), "r2": r2}
        for term, label in WEATHER_TERMS:
            if term not in names:
                continue
            i = names.index(term)
            n_term = int(x[:, i].sum())
            rows.append(
                {
                    "mode": mode,
                    "term": term,
                    "label": label,
                    "effect_pct": 100 * (np.exp(beta[i]) - 1),
                    "ci_low": 100 * (np.exp(beta[i] - 1.96 * se[i]) - 1),
                    "ci_high": 100 * (np.exp(beta[i] + 1.96 * se[i]) - 1),
                    "n_hours": n_term,
                }
            )
    return pd.DataFrame(rows), fit


def naive_rain_gap(df: pd.DataFrame) -> pd.DataFrame:
    """Rainy vs dry hours, same mode/hour/day of week only (no season control), for contrast."""
    d = df.assign(rainy=(df["precipitation"] >= 0.1) & (df["snowfall"] == 0))
    d = d[(df["snowfall"] == 0)]
    cell = d.groupby(["transport_mode", "day_of_week", "hour", "rainy"])["passages"].mean()
    cell = cell.unstack("rainy").dropna()
    ratio = (cell[True] / cell[False]).groupby("transport_mode").mean()
    return (100 * (ratio - 1)).rename("naive_pct").reset_index()


def chart_effects(effects: pd.DataFrame, path: Path) -> None:
    terms = [t for t, _ in WEATHER_TERMS if t in set(effects["term"])]
    labels = dict(WEATHER_TERMS)
    fig, ax = plt.subplots(figsize=(9, 5.5))
    offsets = {"road": -0.25, "rail": 0.0, "sea": 0.25}
    colors = {"road": "#2a6f97", "rail": "#c0504d", "sea": "#3b9b6d"}
    for mode in MODES:
        sub = effects[effects["mode"] == mode].set_index("term").reindex(terms)
        ypos = np.arange(len(terms)) + offsets[mode]
        ax.errorbar(
            sub["effect_pct"],
            ypos,
            xerr=[sub["effect_pct"] - sub["ci_low"], sub["ci_high"] - sub["effect_pct"]],
            fmt="o",
            color=colors[mode],
            label=MODE_LABELS[mode],
            capsize=3,
        )
    ax.axvline(0, color="#888", lw=1)
    ax.set_yticks(np.arange(len(terms)), [labels[t] for t in terms])
    ax.invert_yaxis()
    ax.set_xlabel("Change in hourly passages vs. a dry, mild (15–25 °C), calm hour (%)")
    ax.set_title("Weather effect on Istanbul public transport, 95% CI")
    ax.grid(axis="x", alpha=0.3)
    ax.legend(loc="upper left", frameon=False)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def chart_daily(warehouse: Path, path: Path) -> None:
    with duckdb.connect(str(warehouse), read_only=True) as con:
        daily = con.execute(
            """
            SELECT date, sum(passages) AS passages, max(total_precipitation) AS precipitation
            FROM agg_daily_ridership_weather GROUP BY date ORDER BY date
            """
        ).df()
    fig, ax = plt.subplots(figsize=(11, 4))
    millions = daily["passages"] / 1e6
    ax.plot(daily["date"], millions, color="#2a6f97", lw=0.6, alpha=0.35, label="Daily")
    ax.plot(
        daily["date"],
        millions.rolling(7, center=True).mean(),
        color="#2a6f97",
        lw=1.8,
        label="7-day average",
    )
    ax.legend(loc="lower left", frameon=False)
    ax.set_ylabel("Daily passages (millions)")
    ax2 = ax.twinx()
    ax2.bar(daily["date"], daily["precipitation"], color="#7fb3d5", alpha=0.6, width=1)
    ax2.set_ylabel("Precipitation (mm/day)")
    ax2.set_ylim(0, max(daily["precipitation"].max() * 3, 1))
    ax.set_zorder(ax2.get_zorder() + 1)
    ax.patch.set_visible(False)
    ax.set_title("Daily public transport passages and precipitation, Istanbul")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def write_markdown(effects: pd.DataFrame, fit: dict, naive: pd.DataFrame, df: pd.DataFrame) -> str:
    first, last = (pd.Timestamp(d).date() for d in (df["date"].min(), df["date"].max()))
    lines = [
        "# Weather effect on Istanbul public transport",
        "",
        f"Data: {first} to {last}, hours {FIRST_HOUR}:00–{LAST_HOUR}:59, "
        "İBB hourly passages × Open-Meteo hourly weather. Generated by "
        "`analysis/weather_effect.py`.",
        "",
        "Model per transport mode: `log(passages) ~ weather terms + day-of-week × hour FE + "
        "holiday × hour FE + year-month FE`, standard errors clustered by date. Effects are "
        "the % change in hourly passages relative to a dry, 15–25 °C hour with wind below 30 km/h.",
        "",
        "| Term | " + " | ".join(MODE_LABELS[m] for m in MODES if m in fit) + " |",
        "|---|" + "---|" * len(fit),
    ]
    for term, label in WEATHER_TERMS:
        cells = []
        for mode in MODES:
            if mode not in fit:
                continue
            row = effects[(effects["mode"] == mode) & (effects["term"] == term)]
            if row.empty:
                cells.append("–")
                continue
            r = row.iloc[0]
            cells.append(
                f"{r.effect_pct:+.1f}% [{r.ci_low:+.1f}, {r.ci_high:+.1f}] (n={r.n_hours:,})"
            )
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    lines += [
        "",
        "| Fit | " + " | ".join(MODE_LABELS[m] for m in MODES if m in fit) + " |",
        "|---|" + "---|" * len(fit),
        "| Hours | " + " | ".join(f"{fit[m]['n_hours']:,}" for m in MODES if m in fit) + " |",
        "| Days (clusters) | "
        + " | ".join(f"{fit[m]['n_days']:,}" for m in MODES if m in fit)
        + " |",
        "| R² (log passages) | "
        + " | ".join(f"{fit[m]['r2']:.3f}" for m in MODES if m in fit)
        + " |",
        "",
        "## Why control for season?",
        "",
        "Naive comparison of rainy vs dry hours with the same mode, hour and weekday, "
        "without month fixed effects:",
        "",
        "| Mode | Naive rainy-hour change |",
        "|---|---|",
        *[f"| {MODE_LABELS[r.transport_mode]} | {r.naive_pct:+.1f}% |" for r in naive.itertuples()],
        "",
        "The naive gap lumps all rain intensities together and mixes rain with the cold and the "
        "season it comes with; the model separates intensity, temperature, wind and month.",
        "",
        "![Weather effects](../img/weather_effects.png)",
        "",
        "![Daily ridership](../img/daily_ridership.png)",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    warehouse = config.WAREHOUSE_PATH
    df = load(warehouse)
    if df.empty:
        raise SystemExit(f"No data in {warehouse}; run ingestion and `dbt build` first.")
    effects, fit = estimate(df)
    naive = naive_rain_gap(df)

    IMG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    chart_effects(effects, IMG_DIR / "weather_effects.png")
    chart_daily(warehouse, IMG_DIR / "daily_ridership.png")
    OUT_MD.write_text(write_markdown(effects, fit, naive, df), encoding="utf-8")
    effects.to_csv(OUT_MD.parent / "weather_effects.csv", index=False)
    print(f"Wrote {OUT_MD} and charts in {IMG_DIR}")


if __name__ == "__main__":
    main()
