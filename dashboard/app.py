"""Streamlit dashboard for persisted headline sentiment and feed analytics."""

import html
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import streamlit as st

from pymongo import MongoClient, DESCENDING

NEPAL_TZ = ZoneInfo("Asia/Kathmandu")



st.set_page_config(
    page_title="Newsroom Pulse",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
    :root {
        --ink: #edf2ff;
        --muted: #a5b1c8;
        --line: #29344a;
        --surface: #141d30;
        --accent: #918bff;
    }
    [data-testid="stAppViewContainer"] { background: #0b1020; color: var(--ink); }
    [data-testid="stHeader"] { background: transparent; }
    [data-testid="stSidebar"] { background: #11182a; border-right: 1px solid var(--line); }
    .block-container { max-width: 1440px; padding-top: 2rem; padding-bottom: 3rem; }
    .hero {
        padding: 30px 34px;
        margin-bottom: 24px;
        border-radius: 18px;
        color: white;
        background: linear-gradient(115deg, #17223b 0%, #343c76 64%, #625bf6 100%);
        box-shadow: 0 14px 36px rgba(0, 0, 0, .28);
    }
    .hero-kicker { color: #c7ceff; font-size: 11px; font-weight: 750; letter-spacing: .16em; }
    .hero h1 { color: white; font-size: 34px; margin: 8px 0 5px; letter-spacing: -.035em; }
    .hero p { color: #e0e5ff; font-size: 14px; margin: 0; }
    .section-heading { color: var(--ink); font-size: 19px; font-weight: 750; margin: 8px 0 14px; }
    .story-card {
        background: var(--surface);
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 16px 19px;
        margin: 0 0 10px;
        transition: border-color .15s ease, transform .15s ease;
    }
    .story-meta { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-bottom: 8px; }
    .story-date { color: var(--muted); font-size: 11px; font-weight: 650; margin-left: auto; }
    .story-score { color: var(--accent); font-size: 27px; font-weight: 780; line-height: 1.1; margin-top: 10px; }
    .sentiment-chip { border-radius: 999px; padding: 4px 9px; font-size: 10px; font-weight: 750; letter-spacing: .04em; }
    .analytics-metric {
        background: var(--surface);
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 13px 15px;
        min-height: 96px;
    }
    .analytics-metric-label { color: var(--muted); font-size: 10px; font-weight: 700; letter-spacing: .06em; }
    .analytics-metric-value { color: var(--ink); font-size: 23px; font-weight: 760; margin-top: 7px; }
    .analytics-metric-note { color: var(--muted); font-size: 10px; margin-top: 2px; }
    .positive { color: #83e0bd; background: #12372f; }
    .negative { color: #ff9ba1; background: #40262f; }
    .neutral { color: #bdc8dc; background: #29344a; }
    .other { color: #cfb9ff; background: #302546; }
    .source-label { color: #c7ceff; background: #202947; border-radius: 999px; padding: 4px 9px; font-size: 10px; font-weight: 750; letter-spacing: .04em; }
    .story-card h3 { color: var(--ink); font-size: 15px; line-height: 1.45; margin: 0; font-weight: 650; }
    .story-card a { color: inherit; text-decoration: none; }
    .story-card a:hover { color: var(--accent); }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_collection():
    """Return the MongoDB collection after ensuring dashboard query indexes."""
    client = MongoClient(os.environ.get("MONGO_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=3000)
    collection = client["newsdb"]["sentiments"]
    collection.create_index("ingested_at")
    collection.create_index("region")
    collection.create_index([("region", 1), ("published_at", 1)])
    return collection


def build_sentiment_chart_data(frame, buckets, frequency, label_format):
    """Count sentiment labels for each requested Nepal-time bucket."""
    dated = frame.dropna(subset=["published_at_nepal"]).set_index("published_at_nepal")
    grouped = dated.groupby(
        [pd.Grouper(freq=frequency), "sentiment"]
    ).size()
    full_index = pd.MultiIndex.from_product(
        [buckets, ["Positive", "Neutral", "Negative"]],
        names=["published_at_nepal", "sentiment"],
    )
    chart_data = (
        grouped.reindex(full_index, fill_value=0)
        .rename("stories")
        .reset_index()
    )
    chart_data["period"] = chart_data["published_at_nepal"].dt.strftime(label_format)
    return chart_data


@st.fragment(run_every="10s")
def render_dashboard():
    """Render recent headlines and analytics for the selected region."""
    selected_region = st.radio(
        "News feed", ["International", "Nepali"], horizontal=True, key="news_region"
    )
    region_filter = (
        {"region": "Nepali"}
        if selected_region == "Nepali"
        else {"$or": [{"region": "International"}, {"region": None}]}
    )
    try:
        collection = get_collection()
        all_records_estimate = collection.estimated_document_count()
        region_records = collection.count_documents(region_filter)
        recent_ingestions = collection.count_documents(
            {
                **region_filter,
                "ingested_at": {"$gte": datetime.now(timezone.utc) - timedelta(hours=1)},
            }
        )
        records = list(
            collection.find(
                region_filter,
                {
                    "title": 1,
                    "link": 1,
                    "source": 1,
                    "region": 1,
                    "sentiment": 1,
                    "sentiment_score": 1,
                    "text": 1,
                    "published_at": 1,
                },
            )
            .sort([("published_at", DESCENDING), ("_id", DESCENDING)])
            .limit(500)
        )
        now_utc = datetime.now(timezone.utc)
        last_hour = now_utc - timedelta(minutes=60)
        last_day = now_utc - timedelta(hours=24)
        sentiment_windows = list(
            collection.aggregate(
                [
                    {
                        "$match": {
                            **region_filter,
                            "published_at": {"$gte": last_day, "$lt": now_utc},
                            "sentiment_score": {"$type": "number"},
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "avg_24h": {"$avg": "$sentiment_score"},
                            "count_24h": {"$sum": 1},
                            "sum_60m": {
                                "$sum": {
                                    "$cond": [
                                        {"$gte": ["$published_at", last_hour]},
                                        "$sentiment_score",
                                        0,
                                    ]
                                }
                            },
                            "count_60m": {
                                "$sum": {
                                    "$cond": [
                                        {"$gte": ["$published_at", last_hour]},
                                        1,
                                        0,
                                    ]
                                }
                            },
                        }
                    },
                ]
            )
        )
    except Exception as exc:
        st.error(f"Could not load stories from MongoDB: {exc}")
        return
    sentiment_window = sentiment_windows[0] if sentiment_windows else {}
    count_last_hour = sentiment_window.get("count_60m", 0)
    count_last_day = sentiment_window.get("count_24h", 0)
    average_last_hour = (
        sentiment_window["sum_60m"] / count_last_hour
        if count_last_hour
        else None
    )
    average_last_day = sentiment_window.get("avg_24h")

    stories = pd.DataFrame(records)
    for field in ("title", "link", "source", "sentiment", "text"):
        if field not in stories:
            stories[field] = ""
        stories[field] = stories[field].fillna("").astype(str)
    if "region" not in stories:
        stories["region"] = selected_region
    else:
        stories["region"] = stories["region"].fillna("International").replace("", "International")
    if "sentiment_score" not in stories:
        stories["sentiment_score"] = pd.NA
    stories["sentiment_score"] = pd.to_numeric(stories["sentiment_score"], errors="coerce")
    if "published_at" not in stories:
        stories["published_at"] = pd.NaT
    stories["published_at"] = pd.to_datetime(stories["published_at"], utc=True, errors="coerce")
    stories = stories.sort_values("published_at", ascending=False, na_position="last", kind="stable")
    stories["published_at_nepal"] = stories["published_at"].dt.tz_convert(NEPAL_TZ)

    home_tab, analytics_tab = st.tabs(["Home", "Analytics"])
    with home_tab:
        hero_columns = st.columns([2.2, 1, 1])
        with hero_columns[0]:
            st.markdown(
                """
                <div class="hero">
                  <div class="hero-kicker">NEWS SENTIMENT</div>
                  <h1>Newsroom Pulse</h1>
                  <p>Recent headlines and their sentiment scores.</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
        for column, (label, average, count) in zip(
            hero_columns[1:],
            (
                ("AVG SENTIMENT · LAST 60 MIN", average_last_hour, count_last_hour),
                ("AVG SENTIMENT · LAST 24 HOURS", average_last_day, count_last_day),
            ),
        ):
            value = f"{average:+.1f}%" if average is not None else "—"
            headline_label = "headline" if count == 1 else "headlines"
            column.markdown(
                f'<div class="analytics-metric">'
                f'<div class="analytics-metric-label">{label}</div>'
                f'<div class="analytics-metric-value">{value}</div>'
                f'<div class="analytics-metric-note">'
                f'{count:,} scored {headline_label} · by publish time</div></div>',
                unsafe_allow_html=True,
            )
        st.markdown('<div class="section-heading">Recent news</div>', unsafe_allow_html=True)
        if stories.empty:
            st.info("No stories yet. Start the producer and stream processor.")
        else:
            for row in stories.head(30).itertuples(index=False):
                sentiment = row.sentiment
                sentiment_class = (
                    sentiment.lower()
                    if sentiment.lower() in {"positive", "negative", "neutral"}
                    else "other"
                )
                title = html.escape(row.title or "Untitled story")
                source = html.escape(row.source.strip() or "Unknown source")
                link = html.escape(row.link, quote=True)
                published = row.published_at_nepal
                published_label = (
                    published.strftime("%d %b %Y · %I:%M %p")
                    if pd.notna(published)
                    else "Published time unavailable"
                )
                headline = (
                    f'<a href="{link}" target="_blank" rel="noopener noreferrer">{title} ↗</a>'
                    if link.startswith(("http://", "https://"))
                    else title
                )
                score = (
                    f"{row.sentiment_score:+.1f}%"
                    if pd.notna(row.sentiment_score)
                    else "—"
                )
                st.markdown(
                    f'<div class="story-card"><div class="story-meta">'
                    f'<span class="sentiment-chip {sentiment_class}">'
                    f'{html.escape(sentiment.upper() or "UNKNOWN")}</span>'
                    f'<span class="source-label">Source: {source}</span>'
                    f'<span class="story-date">{html.escape(published_label)}</span></div>'
                    f'<h3>{headline}</h3><div class="story-score">{score}</div></div>',
                    unsafe_allow_html=True,
                )

    with analytics_tab:
        st.markdown(
            '<div class="section-heading">Sentiment overview</div>',
            unsafe_allow_html=True,
        )
        st.caption(
            f"{selected_region} feed · newest {len(stories):,} records shown, capped at 500."
        )
        if stories.empty:
            st.info("Analytics will appear when stories arrive.")
        else:
            palette = {
                "Positive": "#36b58b",
                "Neutral": "#8492a8",
                "Negative": "#ed777b",
                "Unscored": "#aeb4c0",
            }
            sample_size = len(stories)
            text_coverage = stories["text"].str.strip().ne("").mean()
            scored_coverage = stories["sentiment_score"].notna().mean()
            mean_score = stories["sentiment_score"].mean()
            metrics = [
                ("RECORDS IN FEED", f"{region_records:,}", f"of {all_records_estimate:,} across both feeds"),
                (
                    "INGESTED / HOUR",
                    f"{recent_ingestions:,}",
                    f"{recent_ingestions / 60:.1f} records/min over 60 min",
                ),
                (
                    "RSS TEXT COVERAGE",
                    f"{text_coverage:.0%}",
                    f"with excerpt · latest {sample_size:,}",
                ),
                (
                    "SCORING COVERAGE",
                    f"{scored_coverage:.0%}",
                    f"with score · latest {sample_size:,}",
                ),
                (
                    "MEAN SENTIMENT",
                    f"{mean_score:+.1f}%" if pd.notna(mean_score) else "—",
                    f"among scored stories · latest {sample_size:,}",
                ),
            ]
            metric_columns = st.columns(len(metrics))
            for column, (label, value, note) in zip(metric_columns, metrics):
                column.markdown(
                    f'<div class="analytics-metric">'
                    f'<div class="analytics-metric-label">{label}</div>'
                    f'<div class="analytics-metric-value">{value}</div>'
                    f'<div class="analytics-metric-note">{note}</div></div>',
                    unsafe_allow_html=True,
                )

            chart_columns = st.columns([1, 1.35])
            with chart_columns[0]:
                sentiment_counts = (
                    stories["sentiment"]
                    .value_counts()
                    .rename_axis("sentiment")
                    .reset_index(name="stories")
                )
                fig = px.pie(
                    sentiment_counts,
                    values="stories",
                    names="sentiment",
                    hole=0.68,
                    color="sentiment",
                    color_discrete_map=palette,
                )
                fig.update_traces(
                    textinfo="percent",
                    textfont_size=12,
                    marker_line_color="#141d30",
                    marker_line_width=3,
                )
                fig.update_layout(
                    height=320,
                    margin=dict(t=12, b=12, l=4, r=4),
                    legend=dict(
                        orientation="h",
                        x=0,
                        y=-0.08,
                        font=dict(color="#a5b1c8"),
                    ),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font_color="#a5b1c8",
                )
                with st.container(border=True):
                    st.markdown("**Sentiment mix**")
                    st.plotly_chart(
                        fig,
                        use_container_width=True,
                        config={"displayModeBar": False},
                    )

            with chart_columns[1]:
                source_counts = (
                    stories["source"].value_counts().rename_axis("source").reset_index(name="stories")
                )
                fig = px.bar(
                    source_counts,
                    x="stories",
                    y="source",
                    orientation="h",
                    color_discrete_sequence=["#817aff"],
                )
                fig.update_traces(
                    marker_line_width=0,
                    hovertemplate="%{y}: %{x} stories<extra></extra>",
                )
                fig.update_layout(
                    height=320,
                    margin=dict(t=12, b=12, l=4, r=14),
                    xaxis=dict(title=None, showgrid=True, gridcolor="#29344a", zeroline=False),
                    yaxis=dict(title=None, autorange="reversed"),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font_color="#a5b1c8",
                    showlegend=False,
                )
                with st.container(border=True):
                    st.markdown("**Stories by source**")
                    st.plotly_chart(
                        fig,
                        use_container_width=True,
                        config={"displayModeBar": False},
                    )

            st.markdown(
                '<div class="section-heading">Hourly and daily sentiment</div>',
                unsafe_allow_html=True,
            )
            now = pd.Timestamp.now(tz=NEPAL_TZ)
            time_series_specs = [
                (
                    "Hourly",
                    pd.date_range(
                        now.floor("h") - pd.Timedelta(hours=23),
                        now.floor("h"),
                        freq="h",
                    ),
                    "h",
                    "%d %b %I %p",
                    8,
                ),
                (
                    "Daily",
                    pd.date_range(
                        now.normalize() - pd.Timedelta(days=6),
                        now.normalize(),
                        freq="D",
                    ),
                    "D",
                    "%a %d %b",
                    7,
                ),
            ]
            timeline_columns = st.columns(2)
            for column, (label, buckets, frequency, label_format, tick_count) in zip(
                timeline_columns, time_series_specs
            ):
                chart_data = build_sentiment_chart_data(
                    stories, buckets, frequency, label_format
                )
                fig = px.line(
                    chart_data,
                    x="period",
                    y="stories",
                    color="sentiment",
                    markers=True,
                    color_discrete_map=palette,
                    category_orders={
                        "period": chart_data["period"].drop_duplicates().tolist(),
                        "sentiment": ["Positive", "Neutral", "Negative"],
                    },
                )
                fig.update_layout(
                    height=340,
                    margin=dict(t=12, b=12, l=4, r=12),
                    xaxis=dict(title=None, nticks=tick_count),
                    yaxis=dict(title="Stories", rangemode="tozero"),
                    legend=dict(
                        orientation="h",
                        x=0,
                        y=-0.25,
                        font=dict(color="#a5b1c8"),
                    ),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font_color="#a5b1c8",
                )
                with column:
                    st.markdown(f"**{label} sentiment**")
                    st.plotly_chart(
                        fig,
                        use_container_width=True,
                        config={"displayModeBar": False},
                    )


render_dashboard()
