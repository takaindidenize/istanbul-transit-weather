"""Streamlit dashboard: how does weather change the way Istanbul uses public transport?

Run with ``streamlit run dashboard/app.py``. Reads the dbt marts read-only from the DuckDB
warehouse at ``transit_weather.config.WAREHOUSE_PATH``; all SQL lives in ``queries.py``.
"""

import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

sys.path.insert(0, str(Path(__file__).resolve().parent))
import queries  # noqa: E402
from queries import Filters  # noqa: E402

try:
    from transit_weather.config import WAREHOUSE_PATH
except ImportError:  # package not installed: fall back to the same default as config.py
    _data_dir = Path(os.getenv("TW_DATA_DIR", Path(__file__).resolve().parents[1] / "data"))
    WAREHOUSE_PATH = _data_dir / "warehouse.duckdb"

# Fixed colour per mode (validated categorical slots 1-3), so a filter never repaints a mode.
MODE_COLORS = {"road": "#2a78d6", "rail": "#eb6834", "sea": "#1baf7a"}
MODE_LABELS = {"road": "Road (bus, metrobus)", "rail": "Rail (metro, tram, Marmaray)", "sea": "Sea"}
PRECIP_COLOR = "#6250d6"
# Diverging scale for index_vs_dry: red = fewer riders than dry, grey = same, blue = more.
DIVERGING = [[0.0, "#e34948"], [0.5, "#f0efec"], [1.0, "#2a78d6"]]
GRID = "rgba(128,128,128,0.18)"

st.set_page_config(page_title="Istanbul Transit × Weather", page_icon="🌧️", layout="wide")


# --------------------------------------------------------------------------------------------
# Data access
# --------------------------------------------------------------------------------------------


@st.cache_data(ttl=300, show_spinner=False)
def _run(query_name: str, warehouse: str, mtime: float, filters: Filters | None) -> Any:
    """Run ``queries.<query_name>`` on a short-lived read-only connection.

    The connection is closed straight away so dbt can write to the warehouse while the
    dashboard is open; ``mtime`` is part of the cache key so a rebuild invalidates the cache.
    """
    fn: Callable[..., Any] = getattr(queries, query_name)
    with queries.connect(warehouse) as con:
        return fn(con) if filters is None else fn(con, filters)


def run(query_name: str, filters: Filters | None = None) -> Any:
    return _run(query_name, str(WAREHOUSE_PATH), WAREHOUSE_PATH.stat().st_mtime, filters)


def warehouse_problem() -> str | None:
    """A user-facing explanation if the warehouse or a mart is missing, else None."""
    if not WAREHOUSE_PATH.exists():
        return (
            f"No warehouse found at `{WAREHOUSE_PATH}`.\n\n"
            "Build it first: materialise the assets in Dagster (`dagster dev`), or run the "
            "ingestion and then `dbt build --project-dir dbt --profiles-dir dbt`. For a quick "
            "look without real data, run `python dbt/scripts/make_fixture_data.py` before "
            "`dbt build`."
        )
    try:
        missing = run("missing_marts")
    except Exception as exc:  # noqa: BLE001 - surface any DuckDB error to the user
        return f"Could not open the warehouse at `{WAREHOUSE_PATH}`: {exc}"
    if missing:
        names = ", ".join(f"`{name}`" for name in missing)
        return f"The warehouse exists but these marts are missing: {names}. Run `dbt build`."
    return None


# --------------------------------------------------------------------------------------------
# Charts
# --------------------------------------------------------------------------------------------


def _style(fig: go.Figure, height: int = 380) -> go.Figure:
    fig.update_layout(
        height=height,
        margin={"l": 10, "r": 10, "t": 30, "b": 10},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0, "title": None},
        hovermode="closest",
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor=GRID, zeroline=False)
    return fig


def _mode_label(mode: str) -> str:
    return MODE_LABELS.get(mode, mode.title())


def rain_effect_chart(df: pd.DataFrame) -> go.Figure:
    df = df.assign(
        change_pct=(df["index_vs_dry"] - 1) * 100, mode=df["transport_mode"].map(_mode_label)
    )
    present = set(df["transport_mode"])
    fig = px.bar(
        df,
        x="condition",
        y="change_pct",
        color="mode",
        barmode="group",
        color_discrete_map={_mode_label(m): c for m, c in MODE_COLORS.items()},
        category_orders={"mode": [_mode_label(m) for m in MODE_COLORS if m in present]},
        custom_data=["index_vs_dry", "n_hours"],
        labels={"condition": "Weather condition", "change_pct": "Passages vs dry hours (%)"},
    )
    fig.update_traces(
        hovertemplate=(
            "%{x}<br>%{y:+.1f}% vs dry (index %{customdata[0]:.3f})"
            "<br>%{customdata[1]:,} hours<extra>%{fullData.name}</extra>"
        ),
    )
    fig.add_hline(y=0, line_width=1, line_color="rgba(128,128,128,0.6)")
    fig.update_layout(bargap=0.25, bargroupgap=0.08)
    return _style(fig)


def heatmap_chart(df: pd.DataFrame) -> go.Figure:
    conditions = list(dict.fromkeys(df["condition"]))  # query orders 'dry' first
    grid = df.pivot(index="condition", columns="hour", values="index_vs_dry").reindex(conditions)
    hours = df.pivot(index="condition", columns="hour", values="n_hours").reindex(conditions)
    spread = max(abs(grid.min().min() - 1), abs(grid.max().max() - 1), 0.01)
    fig = go.Figure(
        go.Heatmap(
            z=grid.values,
            x=grid.columns,
            y=grid.index,
            customdata=hours.values,
            colorscale=DIVERGING,
            zmid=1,
            zmin=1 - spread,
            zmax=1 + spread,
            xgap=2,
            ygap=2,
            colorbar={"title": "index<br>vs dry", "thickness": 12},
            hovertemplate=(
                "%{y}, %{x}:00<br>index vs dry: %{z:.3f}<br>%{customdata:,} hours<extra></extra>"
            ),
        )
    )
    fig.update_xaxes(title="Hour of day", dtick=2, showgrid=False)
    fig.update_yaxes(title=None, showgrid=False, autorange="reversed")  # 'dry' on top
    return _style(fig, height=110 + 45 * len(conditions))


def temperature_chart(df: pd.DataFrame) -> go.Figure:
    band_order = df.groupby("temp_band")["avg_temperature"].mean().sort_values().index.tolist()
    fig = go.Figure()
    for mode in [m for m in MODE_COLORS if m in set(df["transport_mode"])] + [
        m for m in df["transport_mode"].unique() if m not in MODE_COLORS
    ]:
        part = df[df["transport_mode"] == mode].set_index("temp_band").reindex(band_order)
        fig.add_trace(
            go.Scatter(
                x=band_order,
                y=part["ridership_index"],
                name=_mode_label(mode),
                mode="lines+markers",
                line={"width": 2, "color": MODE_COLORS.get(mode)},
                marker={"size": 8, "line": {"width": 2, "color": "white"}},
                customdata=part[["avg_hourly_passengers", "n_hours"]].values,
                hovertemplate=(
                    "%{x}<br>index %{y:.3f}<br>avg %{customdata[0]:,.0f} passengers/hour"
                    "<br>%{customdata[1]:,} hours<extra>%{fullData.name}</extra>"
                ),
            )
        )
    fig.add_hline(y=1, line_width=1, line_dash="dot", line_color="rgba(128,128,128,0.8)")
    fig.update_xaxes(title="Temperature band", type="category")
    fig.update_yaxes(title="Ridership index (1 = typical hour)")
    return _style(fig)


def daily_chart(df: pd.DataFrame) -> go.Figure:
    # Two panels on a shared time axis rather than a dual-axis chart.
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.72, 0.28],
        vertical_spacing=0.06,
    )
    for mode in [m for m in MODE_COLORS if m in set(df["transport_mode"])]:
        part = df[df["transport_mode"] == mode]
        fig.add_trace(
            go.Scatter(
                x=part["date"],
                y=part["passengers"],
                name=_mode_label(mode),
                mode="lines",
                line={"width": 2, "color": MODE_COLORS[mode]},
                hovertemplate="%{y:,.0f}<extra>%{fullData.name}</extra>",
            ),
            row=1,
            col=1,
        )
    weather = df.groupby("date", as_index=False).agg(
        precipitation=("total_precipitation", "max"), temperature=("avg_temperature", "max")
    )
    fig.add_trace(
        go.Bar(
            x=weather["date"],
            y=weather["precipitation"],
            name="Precipitation",
            marker_color=PRECIP_COLOR,
            customdata=weather["temperature"],
            hovertemplate="%{y:.1f} mm, avg %{customdata:.1f} °C<extra>Weather</extra>",
            showlegend=False,
        ),
        row=2,
        col=1,
    )
    _style(fig, height=520)
    fig.update_layout(hovermode="x unified")
    fig.update_yaxes(title="Passengers / day", row=1, col=1)
    fig.update_yaxes(title="Precip. (mm)", row=2, col=1)
    return fig


# --------------------------------------------------------------------------------------------
# Page
# --------------------------------------------------------------------------------------------


def _fmt_int(value: float | int | None) -> str:
    return "–" if value is None or pd.isna(value) else f"{value:,.0f}"


def _fmt_compact(value: float | int | None) -> str:
    """3,800,320,134 -> '3.80B', so large totals fit in a metric tile."""
    if value is None or pd.isna(value):
        return "–"
    for threshold, suffix in ((1e9, "B"), (1e6, "M")):
        if abs(value) >= threshold:
            return f"{value / threshold:.2f}{suffix}"
    return _fmt_int(value)


def sidebar(first, last, modes: list[str]) -> Filters | None:
    st.sidebar.header("Filters")
    chosen_modes = st.sidebar.multiselect(
        "Transport mode", modes, default=modes, format_func=_mode_label
    )
    picked = st.sidebar.date_input(
        "Date range", value=(first, last), min_value=first, max_value=last
    )
    day_choice = st.sidebar.radio("Days", ("All days", "Weekdays", "Weekends & holidays"))
    day_types = {
        "All days": queries.DAY_TYPES,
        "Weekdays": ("weekday",),
        "Weekends & holidays": ("weekend_or_holiday",),
    }[day_choice]
    st.sidebar.caption(
        "The rain-effect and heatmap sections come from a mart aggregated over the whole "
        "period, so the date range does not apply to them."
    )
    if not chosen_modes:
        st.warning("Select at least one transport mode.")
        return None
    if not isinstance(picked, tuple | list) or len(picked) != 2:
        st.info("Pick an end date for the date range.")
        return None
    return Filters(tuple(chosen_modes), picked[0], picked[1], day_types)


def show_data(df: pd.DataFrame, label: str = "Show data") -> None:
    with st.expander(label):
        st.dataframe(df, width="stretch", hide_index=True)


def main() -> None:
    st.title("Istanbul Transit × Weather")
    st.caption(
        "How does weather change the way Istanbul uses public transport? "
        "İBB hourly ridership joined with Open-Meteo hourly weather."
    )

    problem = warehouse_problem()
    if problem:
        st.warning(problem)
        st.stop()

    first, last, modes = run("bounds")
    if first is None:
        st.warning("The marts are empty. Ingest at least one month and run `dbt build`.")
        st.stop()
    filters = sidebar(first, last, modes)
    if filters is None:
        st.stop()

    # Headline KPIs ---------------------------------------------------------------------------
    k = run("kpis", filters)
    cols = st.columns(4)
    cols[0].metric("Passengers", _fmt_compact(k["total_passengers"]))
    cols[1].metric("Avg passengers / day", _fmt_compact(k["avg_daily_passengers"]))
    cols[2].metric(
        f"Wet days (≥ {queries.WET_DAY_MM:g} mm)",
        f"{_fmt_int(k['wet_days'])} / {_fmt_int(k['n_days'])}",
    )
    change = k["wet_day_change"]
    cols[3].metric(
        "Wet day vs dry day",
        "–" if change is None else f"{change * 100:+.1f}%",
        help="Average daily passengers on wet days vs dry days of the same day type.",
    )

    # Rain effect by mode ---------------------------------------------------------------------
    st.subheader("Rain effect by transport mode")
    st.caption(
        "Passages relative to dry hours at the same hour of day and day type "
        "(`index_vs_dry` − 1). Below zero: fewer people travel."
    )
    effect = run("rain_effect_by_mode", filters)
    if effect.empty:
        st.info("No weather-effect rows for this selection.")
    else:
        st.plotly_chart(rain_effect_chart(effect), width="stretch")
        show_data(effect)

    # Hour-of-day × condition heatmap ---------------------------------------------------------
    st.subheader("When does the weather matter? Hour of day × condition")
    heat = run("hour_condition_heatmap", filters)
    if heat.empty:
        st.info("No weather-effect rows for this selection.")
    else:
        heat_modes = [m for m in filters.modes if m in set(heat["transport_mode"])]
        tabs = st.tabs([_mode_label(m) for m in heat_modes])
        for tab, mode in zip(tabs, heat_modes, strict=True):
            with tab:
                st.plotly_chart(
                    heatmap_chart(heat[heat["transport_mode"] == mode]),
                    width="stretch",
                )
        show_data(heat)

    # Temperature band vs ridership -----------------------------------------------------------
    st.subheader("Temperature vs ridership")
    st.caption(
        "Each hour is compared with the average for the same mode, hour of day and "
        "weekday/weekend, so the index isolates temperature from the daily rhythm."
    )
    temp = run("temperature_band_effect", filters)
    if temp.empty:
        st.info("No hourly rows for this selection.")
    else:
        st.plotly_chart(temperature_chart(temp), width="stretch")
        show_data(temp)

    # Daily time series -----------------------------------------------------------------------
    st.subheader("Daily ridership and precipitation")
    daily = run("daily_series", filters)
    if daily.empty:
        st.info("No daily rows for this selection.")
    else:
        st.plotly_chart(daily_chart(daily), width="stretch")
        show_data(daily)

    st.caption(f"Warehouse: `{WAREHOUSE_PATH}`")


main()
