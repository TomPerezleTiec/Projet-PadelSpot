from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, dcc, html, no_update


TOPIC = "padel_club_events"
FRANCE_CENTER = {"lat": 46.6, "lon": 2.3}
FRANCE_ZOOM = 5.0
PROJECT_ROOT = Path("/home/jovyan/work") if Path("/home/jovyan/work").exists() else Path.cwd()
BASE_CLUBS_PATH = PROJECT_ROOT / "data" / "dash_ready" / "dash_clubs.parquet"
STREAMING_CURRENT_PATH = PROJECT_ROOT / "data" / "streaming" / "silver_clubs_current"
STREAMING_JOB_PATH = PROJECT_ROOT / "src" / "padelspot" / "streaming" / "club_events_stream.py"
IMPACT_RADIUS_KM = 55.0
SCORE_DELTA_VISIBLE = 0.008
APP_STARTED_AT = datetime.now(timezone.utc)


DEPARTMENTS = [
    ("59", "Lille", 50.637, 3.063),
    ("75", "Paris", 48.856, 2.352),
    ("69", "Lyon", 45.764, 4.835),
    ("31", "Toulouse", 43.604, 1.444),
    ("34", "Montpellier", 43.611, 3.877),
    ("33", "Bordeaux", 44.837, -0.579),
    ("13", "Marseille", 43.296, 5.369),
    ("44", "Nantes", 47.218, -1.553),
]


producer_state = {
    "running": False,
    "last_message": "Pret a lancer le flux Kafka.",
    "created": 0,
    "updated": 0,
    "deleted": 0,
    "total": 0,
}
producer_lock = threading.Lock()
consumer_process: subprocess.Popen[str] | None = None
consumer_lock = threading.Lock()
parquet_cache: dict[Path, pd.DataFrame] = {}
parquet_cache_lock = threading.Lock()
demo_live_clubs: dict[str, dict[str, Any]] = {}
demo_live_lock = threading.Lock()


def _default_bootstrap_servers() -> str:
    if os.environ.get("KAFKA_BOOTSTRAP_SERVERS"):
        return os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    if Path("/.dockerenv").exists():
        return "kafka:9092"
    return "localhost:9094"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        with parquet_cache_lock:
            return parquet_cache.get(path, pd.DataFrame()).copy()
    try:
        df = pd.read_parquet(path)
        with parquet_cache_lock:
            parquet_cache[path] = df.copy()
        return df
    except Exception:
        with parquet_cache_lock:
            return parquet_cache.get(path, pd.DataFrame()).copy()


def _normalize_base_clubs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    for col in [
        "nom",
        "type",
        "commune",
        "latitude",
        "longitude",
        "nombre_de_courts",
        "source",
        "departement_code",
        "score_zone_implantation",
        "score_accessibilite_zone",
        "ind_snv_zone",
        "prix_median_m2_zone",
        "indice_demande_trends_zone",
        "part_cible_padel_zone",
        "zone_saturee",
    ]:
        if col not in out:
            out[col] = np.nan

    out["stream_source"] = "Batch"
    out["stream_badge"] = "Historique"
    out["event_time"] = ""
    out["club_id"] = out["nom"].fillna("").astype(str)
    return out


def _normalize_streaming_clubs(df: pd.DataFrame, base: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=_normalize_base_clubs(base).columns)

    if "event_time" in df:
        event_times = pd.to_datetime(df["event_time"], errors="coerce", utc=True)
        df = df[event_times >= APP_STARTED_AT].copy()
        if df.empty:
            return pd.DataFrame(columns=_normalize_base_clubs(base).columns)

    out = pd.DataFrame(
        {
            "nom": df.get("name", pd.Series(dtype=str)),
            "type": "Club streaming",
            "commune": df.get("city", pd.Series(dtype=str)),
            "latitude": df.get("latitude", pd.Series(dtype=float)),
            "longitude": df.get("longitude", pd.Series(dtype=float)),
            "nombre_de_courts": df.get("courts", pd.Series(dtype=float)),
            "source": "Kafka",
            "departement_code": df.get("department", pd.Series(dtype=str)),
            "event_time": df.get("event_time", pd.Series(dtype=str)),
            "club_id": df.get("club_id", pd.Series(dtype=str)),
        }
    )

    base_scores = base.copy()
    if not base_scores.empty:
        dept_medians = (
            base_scores.groupby("departement_code", dropna=False)
            .agg(
                score_zone_implantation=("score_zone_implantation", "median"),
                score_accessibilite_zone=("score_accessibilite_zone", "median"),
                ind_snv_zone=("ind_snv_zone", "median"),
                prix_median_m2_zone=("prix_median_m2_zone", "median"),
                indice_demande_trends_zone=("indice_demande_trends_zone", "median"),
                part_cible_padel_zone=("part_cible_padel_zone", "median"),
            )
            .reset_index()
        )
        out = out.merge(dept_medians, on="departement_code", how="left")
    else:
        for col in [
            "score_zone_implantation",
            "score_accessibilite_zone",
            "ind_snv_zone",
            "prix_median_m2_zone",
            "indice_demande_trends_zone",
            "part_cible_padel_zone",
        ]:
            out[col] = np.nan

    out["zone_saturee"] = False
    out["stream_source"] = "Streaming"
    out["stream_badge"] = "Kafka live"
    return out


def _demo_live_frame() -> pd.DataFrame:
    with demo_live_lock:
        records = list(demo_live_clubs.values())
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


def load_clubs_dataframe() -> pd.DataFrame:
    base = _normalize_base_clubs(_read_parquet(BASE_CLUBS_PATH))
    streaming_from_parquet = _normalize_streaming_clubs(_read_parquet(STREAMING_CURRENT_PATH), base)
    streaming_from_app = _normalize_streaming_clubs(_demo_live_frame(), base)
    streaming = pd.concat([streaming_from_parquet, streaming_from_app], ignore_index=True, sort=False)
    if not streaming.empty and "club_id" in streaming:
        streaming = streaming.drop_duplicates(subset=["club_id"], keep="last")
    data = pd.concat([base, streaming], ignore_index=True, sort=False)

    if data.empty:
        return data

    data["latitude"] = pd.to_numeric(data["latitude"], errors="coerce")
    data["longitude"] = pd.to_numeric(data["longitude"], errors="coerce")
    for col in [
        "nombre_de_courts",
        "score_zone_implantation",
        "prix_median_m2_zone",
        "indice_demande_trends_zone",
        "part_cible_padel_zone",
    ]:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    data["departement_code"] = data["departement_code"].fillna("N/A").astype(str)
    data["commune"] = data["commune"].fillna("N/A").astype(str)
    data["nom"] = data["nom"].fillna("Sans nom").astype(str)
    data["type"] = data["type"].fillna("N/A").astype(str)
    data["stream_source"] = data["stream_source"].fillna("Batch").astype(str)
    data = data[data["latitude"].between(41.0, 51.5) & data["longitude"].between(-5.5, 10.5)]
    return _apply_live_score(data)


def _haversine_km(lat1: pd.Series, lon1: pd.Series, lat2: float, lon2: float) -> np.ndarray:
    earth_radius_km = 6371.0
    lat1_rad = np.radians(pd.to_numeric(lat1, errors="coerce").to_numpy(dtype=float))
    lon1_rad = np.radians(pd.to_numeric(lon1, errors="coerce").to_numpy(dtype=float))
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)
    dlat = lat1_rad - lat2_rad
    dlon = lon1_rad - lon2_rad
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2.0) ** 2
    return 2 * earth_radius_km * np.arcsin(np.sqrt(a))


def _apply_live_score(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return data

    out = data.copy()
    base_score = pd.to_numeric(out["score_zone_implantation"], errors="coerce")
    valid_base_scores = base_score[base_score > 0]
    fallback_score = float(valid_base_scores.median()) if not valid_base_scores.empty else 0.35
    out["score_base"] = base_score.where(base_score > 0, fallback_score).fillna(fallback_score)
    out["competition_pressure"] = 0.0
    out["distance_to_live_club_km"] = np.nan

    live = out[out["stream_source"] == "Streaming"].dropna(subset=["latitude", "longitude"])
    if not live.empty:
        min_distance = np.full(len(out), np.inf)
        pressure = np.zeros(len(out), dtype=float)
        out_ids = out["club_id"].astype(str).to_numpy()

        for _, club in live.iterrows():
            distances = _haversine_km(out["latitude"], out["longitude"], float(club["latitude"]), float(club["longitude"]))
            same_club = out_ids == str(club.get("club_id", ""))
            in_radius = (distances <= IMPACT_RADIUS_KM) & ~same_club
            courts = pd.to_numeric(pd.Series([club.get("nombre_de_courts")]), errors="coerce").fillna(2).iloc[0]
            courts_factor = min(max(float(courts), 1.0), 12.0) / 12.0
            intensity = 0.035 + courts_factor * 0.16
            distance_factor = (1.0 - distances / IMPACT_RADIUS_KM).clip(min=0.0) ** 1.25
            local_pressure = distance_factor * intensity
            pressure += np.where(in_radius, local_pressure, 0.0)
            min_distance = np.where(in_radius & (distances < min_distance), distances, min_distance)

        out["competition_pressure"] = np.clip(pressure, 0.0, 0.45)
        out["distance_to_live_club_km"] = np.where(np.isfinite(min_distance), min_distance, np.nan)

    out["score"] = (out["score_base"] - out["competition_pressure"]).clip(lower=0.02, upper=1.0)
    out["score_live"] = out["score"]
    out["score_delta"] = -out["competition_pressure"]
    out["score_impacted"] = out["competition_pressure"] >= SCORE_DELTA_VISIBLE
    return out


def _current_streaming_state() -> list[dict[str, Any]]:
    df = _read_parquet(STREAMING_CURRENT_PATH)
    if df.empty or "club_id" not in df:
        return []
    records = []
    for record in df.to_dict("records"):
        if record.get("club_id"):
            records.append(record)
    return records


def _send_event(event: dict[str, Any], bootstrap_servers: str) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    producer.send(TOPIC, key=event["club_id"], value=event).get(timeout=30)
    producer.flush()
    producer.close()


def _random_department() -> tuple[str, str, float, float]:
    return random.choice(DEPARTMENTS)


def _random_anchor() -> tuple[str, str, float, float]:
    base = _normalize_base_clubs(_read_parquet(BASE_CLUBS_PATH))
    if not base.empty:
        base = base.dropna(subset=["latitude", "longitude"])
        if not base.empty:
            row = base.sample(1).iloc[0]
            return (
                str(row["departement_code"]),
                str(row["commune"]),
                float(row["latitude"]),
                float(row["longitude"]),
            )
    return _random_department()


def _random_event(index: int, existing_clubs: list[dict[str, Any]]) -> dict[str, Any]:
    if not existing_clubs:
        action = "created"
    else:
        action = random.choices(["created", "updated", "deleted"], weights=[0.64, 0.34, 0.02])[0]

    department, city, lat, lon = _random_anchor()
    if existing_clubs and random.random() < 0.22:
        anchor = random.choice(existing_clubs)
        department = str(anchor.get("department") or department)
        city = str(anchor.get("city") or city)
        lat = float(anchor.get("latitude") or lat)
        lon = float(anchor.get("longitude") or lon)
    if action == "created":
        club_id = f"dash_live_{uuid.uuid4().hex[:8]}"
        courts = random.choices([2, 3, 4, 5, 6, 8, 10], weights=[2, 3, 4, 4, 5, 5, 4])[0]
        jitter = random.choices([0.035, 0.07, 0.12, 0.18], weights=[5, 6, 4, 2])[0]
        club_state = {
            "club_id": club_id,
            "name": f"{random.choice(['Padel', 'Urban Padel', 'Padel Club', 'Arena Padel'])} {club_id[-4:].upper()}",
            "city": city,
            "department": department,
            "latitude": round(lat + random.uniform(-jitter, jitter), 6),
            "longitude": round(lon + random.uniform(-jitter, jitter), 6),
            "courts": courts,
        }
        existing_clubs.append(club_state)
    else:
        club_state = dict(random.choice(existing_clubs))
        club_id = str(club_state["club_id"])
        if action == "deleted":
            existing_clubs[:] = [club for club in existing_clubs if str(club.get("club_id")) != club_id]
        else:
            update_fields = random.sample(
                ["name", "courts", "move_nearby", "relocate"],
                k=random.randint(1, 3),
            )
            if "name" in update_fields:
                club_state["name"] = f"{random.choice(['Padel', 'Urban Padel', 'Padel Center', 'Padel Factory'])} {random.choice(['Nord', 'Sud', 'Est', 'Ouest', club_id[-4:].upper()])}"
            if "courts" in update_fields:
                club_state["courts"] = random.choices([2, 3, 4, 5, 6, 8, 10], weights=[2, 3, 4, 4, 5, 5, 4])[0]
            if "move_nearby" in update_fields:
                club_state["latitude"] = round(float(club_state.get("latitude") or lat) + random.uniform(-0.05, 0.05), 6)
                club_state["longitude"] = round(float(club_state.get("longitude") or lon) + random.uniform(-0.05, 0.05), 6)
            if "relocate" in update_fields:
                department, city, lat, lon = _random_anchor()
                jitter = random.choices([0.035, 0.07, 0.12, 0.18], weights=[5, 6, 4, 2])[0]
                club_state.update(
                    {
                        "city": city,
                        "department": department,
                        "latitude": round(lat + random.uniform(-jitter, jitter), 6),
                        "longitude": round(lon + random.uniform(-jitter, jitter), 6),
                    }
                )
            for i, club in enumerate(existing_clubs):
                if str(club.get("club_id")) == club_id:
                    existing_clubs[i] = club_state
                    break

    event = {
        "event_id": f"evt_dash_{int(time.time())}_{index}_{uuid.uuid4().hex[:6]}",
        "event_type": f"club_{action}",
        "club_id": club_id,
        "event_time": _now_iso(),
        "source": "dash_streaming_button",
    }
    if action != "deleted":
        event.update(
            {
                "name": club_state.get("name"),
                "city": club_state.get("city"),
                "department": club_state.get("department"),
                "latitude": float(club_state.get("latitude")),
                "longitude": float(club_state.get("longitude")),
                "courts": int(club_state.get("courts") or 2),
            }
        )
    return event


def _ensure_consumer_running() -> None:
    global consumer_process
    with consumer_lock:
        if consumer_process is not None and consumer_process.poll() is None:
            return
        env = os.environ.copy()
        env.setdefault("KAFKA_BOOTSTRAP_SERVERS", _default_bootstrap_servers())
        consumer_process = subprocess.Popen(
            [sys.executable, str(STREAMING_JOB_PATH), "--duration", "3600"],
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )


def _producer_worker(event_count: int, delay: float) -> None:
    bootstrap_servers = _default_bootstrap_servers()
    existing_clubs: list[dict[str, Any]] = []
    with producer_lock:
        producer_state.update(
            {
                "running": True,
                "last_message": "Flux Kafka aleatoire en cours sur toute la France...",
                "created": 0,
                "updated": 0,
                "deleted": 0,
                "total": 0,
            }
        )

    try:
        _ensure_consumer_running()
        for index in range(1, event_count + 1):
            event = _random_event(index, existing_clubs)
            action = event["event_type"].replace("club_", "")
            with demo_live_lock:
                if action == "deleted":
                    demo_live_clubs.pop(event["club_id"], None)
                else:
                    demo_live_clubs[event["club_id"]] = {
                        "club_id": event["club_id"],
                        "name": event.get("name"),
                        "city": event.get("city"),
                        "department": event.get("department"),
                        "latitude": event.get("latitude"),
                        "longitude": event.get("longitude"),
                        "courts": event.get("courts"),
                        "event_time": event.get("event_time"),
                    }
            _send_event(event, bootstrap_servers)
            with producer_lock:
                producer_state["total"] += 1
                producer_state[action] += 1
                producer_state["last_message"] = (
                    f"Dernier event: {event['event_type']} - {event['club_id']}"
                )
            time.sleep(delay)
        with producer_lock:
            producer_state["last_message"] = "Flux termine. La carte se met a jour automatiquement."
    except Exception as exc:
        with producer_lock:
            producer_state["last_message"] = f"Erreur streaming: {exc}"
    finally:
        with producer_lock:
            producer_state["running"] = False


def _start_random_events(event_count: int, delay: float) -> str:
    with producer_lock:
        if producer_state["running"]:
            return "Un flux est deja en cours."

    thread = threading.Thread(target=_producer_worker, args=(event_count, delay), daemon=True)
    thread.start()
    return f"Flux lance: {event_count} events Kafka."


def _filter_data(
    data: pd.DataFrame,
    departments: list[str] | None,
    source_mode: str,
    type_mode: str,
    score_range: list[float],
    commune_query: str,
    zone_mode: str,
    price_range: list[float],
    trends_range: list[float],
) -> pd.DataFrame:
    if data.empty:
        return data

    out = data.copy()
    if departments:
        out = out[out["departement_code"].isin(departments)]
    if source_mode != "all":
        out = out[out["stream_source"] == source_mode]
    if type_mode != "all":
        typ = out["type"].str.lower()
        name = out["nom"].str.lower()
        if type_mode == "streaming":
            out = out[out["stream_source"] == "Streaming"]
        elif type_mode == "tennis_padel":
            out = out[(typ.str.contains("tennis") & typ.str.contains("padel")) | (name.str.contains("tennis") & name.str.contains("padel"))]
        elif type_mode == "piste":
            out = out[(typ.str.contains("piste") & typ.str.contains("padel")) | (name.str.contains("piste") & name.str.contains("padel"))]
        else:
            out = out[typ.str.contains("padel") | name.str.contains("padel")]

    if score_range:
        out = out[out["score_live"].between(score_range[0], score_range[1]) | out["score_live"].isna()]
    if commune_query:
        out = out[out["commune"].str.lower().str.contains(commune_query.lower(), na=False)]
    if zone_mode != "all":
        zone = out["zone_saturee"].astype(str).str.lower()
        if zone_mode == "true":
            out = out[zone.isin(["true", "1", "yes", "oui"])]
        else:
            out = out[zone.isin(["false", "0", "no", "non"])]
    if price_range:
        out = out[out["prix_median_m2_zone"].between(price_range[0], price_range[1]) | out["prix_median_m2_zone"].isna()]
    if trends_range:
        out = out[out["indice_demande_trends_zone"].between(trends_range[0], trends_range[1]) | out["indice_demande_trends_zone"].isna()]
    return out


def _numeric_bounds(data: pd.DataFrame, col: str, fallback: tuple[float, float]) -> tuple[float, float]:
    if data.empty or col not in data:
        return fallback
    values = pd.to_numeric(data[col], errors="coerce").dropna()
    if values.empty:
        return fallback
    low, high = float(values.min()), float(values.max())
    if low == high:
        high = low + 1.0
    return low, high


def _range_marks(low: float, high: float, decimals: int = 0) -> dict[float, str]:
    mid = (low + high) / 2
    return {
        low: f"{low:.{decimals}f}",
        mid: f"{mid:.{decimals}f}",
        high: f"{high:.{decimals}f}",
    }


def _sizes(df: pd.DataFrame) -> np.ndarray:
    courts = pd.to_numeric(df["nombre_de_courts"], errors="coerce").fillna(2)
    return np.clip(4.5 + courts * 0.75, 5.5, 13.0)


def _fmt_number(value: Any, decimals: int = 1, missing_zero: bool = False) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "N/A"
    if not np.isfinite(number) or (missing_zero and number <= 0):
        return "N/A"
    if decimals == 0:
        return f"{number:.0f}"
    return f"{number:.{decimals}f}"


def _display_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["courts_display"] = out["nombre_de_courts"].map(lambda value: _fmt_number(value, 0))
    out["score_display"] = out["score"].map(lambda value: _fmt_number(value, 3))
    out["pressure_display"] = out["competition_pressure"].map(lambda value: _fmt_number(value, 3, missing_zero=False))
    out["distance_display"] = np.where(
        pd.to_numeric(out["competition_pressure"], errors="coerce").fillna(0) > 0,
        out["distance_to_live_club_km"].map(lambda value: _fmt_number(value, 1)),
        "Aucun impact local",
    )
    out["price_display"] = out["prix_median_m2_zone"].map(lambda value: _fmt_number(value, 0, missing_zero=True))
    out["trends_display"] = out["indice_demande_trends_zone"].map(lambda value: _fmt_number(value, 1, missing_zero=True))
    out["event_time_display"] = out["event_time"].replace("", "N/A").fillna("N/A").astype(str)
    return out


def _figure(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if not df.empty:
        batch = df[df["stream_source"] == "Batch"]
        live = df[df["stream_source"] == "Streaming"]
        impacted = df[(df["score_impacted"]) & (df["stream_source"] == "Batch")]
        color_values = pd.to_numeric(df["score"], errors="coerce").dropna()
        if color_values.empty:
            color_min, color_max = 0.0, 1.0
        else:
            color_min = float(color_values.quantile(0.02))
            color_max = float(color_values.quantile(0.98))
            if color_min == color_max:
                color_min = max(0.0, color_min - 0.02)
                color_max = min(1.0, color_max + 0.02)

        if not impacted.empty:
            fig.add_trace(
                go.Scattermap(
                    lat=impacted["latitude"],
                    lon=impacted["longitude"],
                    mode="markers",
                    name="Scores impactes",
                    marker={
                        "size": _sizes(impacted) + 4,
                        "color": "rgba(251,191,36,0.58)",
                        "opacity": 0.55,
                    },
                    hoverinfo="skip",
                )
            )

        for subset, name, opacity, line_color in [
            (batch, "Clubs historiques", 0.72, "rgba(17,24,39,0.42)"),
            (live, "Clubs Kafka live", 0.95, "rgba(255,255,255,0.95)"),
        ]:
            if subset.empty:
                continue
            subset_display = _display_frame(subset)
            marker = {
                "size": _sizes(subset_display),
                "color": subset_display["score"],
                "colorscale": "Turbo",
                "cmin": color_min,
                "cmax": color_max,
                "opacity": opacity,
            }
            if name == "Clubs historiques":
                marker["colorbar"] = {"title": "Score", "thickness": 12, "len": 0.5}
            if name.endswith("live"):
                fig.add_trace(
                    go.Scattermap(
                        lat=subset["latitude"],
                        lon=subset["longitude"],
                        mode="markers",
                        name="Halo Kafka",
                        marker={
                            "size": _sizes(subset_display) + 4,
                            "color": line_color,
                            "opacity": 0.62,
                        },
                        hoverinfo="skip",
                        showlegend=False,
                    )
                )
            custom = np.stack(
                [
                    subset_display["nom"].astype(str),
                    subset_display["commune"].astype(str),
                    subset_display["departement_code"].astype(str),
                    subset_display["type"].astype(str),
                    subset_display["stream_badge"].astype(str),
                    subset_display["courts_display"],
                    subset_display["score_display"],
                    subset_display["pressure_display"],
                    subset_display["distance_display"],
                    subset_display["price_display"],
                    subset_display["trends_display"],
                    subset_display["event_time_display"],
                ],
                axis=-1,
            )
            fig.add_trace(
                go.Scattermap(
                    lat=subset["latitude"],
                    lon=subset["longitude"],
                    mode="markers",
                    name=name,
                    customdata=custom,
                    marker=marker,
                    hovertemplate=(
                        "<b>%{customdata[0]}</b><br>"
                        "Commune: %{customdata[1]}<br>"
                        "Departement: %{customdata[2]}<br>"
                        "Type: %{customdata[3]}<br>"
                        "Source: %{customdata[4]}<br>"
                        "Courts: %{customdata[5]}<br>"
                        "Score: %{customdata[6]}<br>"
                        "Pression locale: %{customdata[7]}<br>"
                        "Club live proche: %{customdata[8]}<br>"
                        "Prix m2: %{customdata[9]}<br>"
                        "Trends: %{customdata[10]}<br>"
                        "Event time: %{customdata[11]}<extra></extra>"
                    ),
                )
            )

    fig.update_layout(
        map={"style": "open-street-map", "center": FRANCE_CENTER, "zoom": FRANCE_ZOOM},
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
        paper_bgcolor="#07111f",
        plot_bgcolor="#07111f",
        font={"family": "Inter, Segoe UI, Arial", "color": "#e5eefb"},
        legend={"orientation": "h", "yanchor": "bottom", "y": 0.02, "xanchor": "left", "x": 0.02, "bgcolor": "rgba(7,17,31,0.72)"},
        uirevision="keep-map-position",
    )
    return fig


def _state_snapshot() -> dict[str, Any]:
    with producer_lock:
        return dict(producer_state)


initial_data = load_clubs_dataframe()
score_min, score_max = _numeric_bounds(initial_data, "score_live", (0.0, 1.0))
price_min, price_max = _numeric_bounds(initial_data, "prix_median_m2_zone", (0.0, 5000.0))
trends_min, trends_max = _numeric_bounds(initial_data, "indice_demande_trends_zone", (0.0, 100.0))
score_marks = _range_marks(score_min, score_max, 2)
price_marks = _range_marks(price_min, price_max, 0)
trends_marks = _range_marks(trends_min, trends_max, 0)
all_departments = sorted(initial_data["departement_code"].dropna().astype(str).unique().tolist()) if not initial_data.empty else []


app = Dash(__name__, title="PadelSpot Streaming")
server = app.server


def _stat_card(title: str, value_id: str, subtitle: str) -> html.Div:
    return html.Div(
        [html.Div(title, className="stat-title"), html.Div(id=value_id, className="stat-value"), html.Div(subtitle, className="stat-subtitle")],
        className="stat-card",
    )


app.layout = html.Div(
    [
        dcc.Interval(id="refresh-interval", interval=2500, n_intervals=0),
        html.Div(
            [
                html.Div(
                    [
                        html.Div("PadelSpot Live", className="eyebrow"),
                        html.H1("Carte streaming des clubs de padel"),
                        html.P("Données batch enrichies par des événements Kafka traités avec Spark Structured Streaming."),
                    ],
                    className="hero-copy",
                ),
                html.Div(
                    [
                        _stat_card("Clubs visibles", "stat-visible", "apres filtres"),
                        _stat_card("Kafka live", "stat-live", "clubs actifs"),
                        _stat_card("Scores impactes", "stat-impacted", f"rayon {int(IMPACT_RADIUS_KM)} km"),
                        _stat_card("Evenements", "stat-events", "session Dash"),
                    ],
                    className="stats-grid",
                ),
            ],
            className="topbar",
        ),
        html.Div(
            [
                html.Aside(
                    [
                        html.Section(
                            [
                                html.H2("Flux Kafka"),
                                html.Label("Nombre d'evenements"),
                                dcc.Slider(id="event-count", min=50, max=3000, step=50, value=500, marks={50: "50", 500: "500", 1500: "1500", 3000: "3000"}),
                                html.Label("Delai entre events"),
                                dcc.Slider(id="event-delay", min=0, max=0.5, step=0.01, value=0.02, marks={0: "0s", 0.02: "0.02s", 0.2: "0.2s", 0.5: "0.5s"}),
                                html.Button("Lancer des events aleatoires", id="start-stream", n_clicks=0, className="primary-button"),
                                html.Div(id="stream-status", className="status"),
                            ],
                            className="panel",
                        ),
                        html.Section(
                            [
                                html.H2("Filtres"),
                                html.Label("Source"),
                                dcc.RadioItems(
                                    id="source-mode",
                                    options=[
                                        {"label": "Toutes", "value": "all"},
                                        {"label": "Batch", "value": "Batch"},
                                        {"label": "Kafka live", "value": "Streaming"},
                                    ],
                                    value="all",
                                    className="segmented",
                                ),
                                html.Label("Departements"),
                                dcc.Dropdown(id="departments", options=[{"label": dep, "value": dep} for dep in all_departments], multi=True, placeholder="Tous les departements"),
                                html.Label("Type"),
                                dcc.Dropdown(
                                    id="type-mode",
                                    options=[
                                        {"label": "Tous", "value": "all"},
                                        {"label": "Padel", "value": "padel"},
                                        {"label": "Tennis / padel", "value": "tennis_padel"},
                                        {"label": "Piste", "value": "piste"},
                                        {"label": "Streaming", "value": "streaming"},
                                    ],
                                    value="all",
                                    clearable=False,
                                ),
                                html.Label("Commune"),
                                dcc.Input(id="commune", type="text", placeholder="Paris, Lille...", debounce=True),
                                html.Label("Zone"),
                                dcc.Dropdown(
                                    id="zone-mode",
                                    options=[
                                        {"label": "Toutes", "value": "all"},
                                        {"label": "Saturee", "value": "true"},
                                        {"label": "Non saturee", "value": "false"},
                                    ],
                                    value="all",
                                    clearable=False,
                                ),
                            ],
                            className="panel",
                        ),
                        html.Section(
                            [
                                html.H2("Scores"),
                                html.Label("Score implantation"),
                                dcc.RangeSlider(
                                    id="score-range",
                                    min=score_min,
                                    max=score_max,
                                    step=max((score_max - score_min) / 200, 0.001),
                                    value=[score_min, score_max],
                                    marks=score_marks,
                                    allowCross=False,
                                    tooltip={"placement": "bottom", "always_visible": False},
                                ),
                                html.Label("Prix median m2"),
                                dcc.RangeSlider(
                                    id="price-range",
                                    min=price_min,
                                    max=price_max,
                                    step=max((price_max - price_min) / 200, 1),
                                    value=[price_min, price_max],
                                    marks=price_marks,
                                    allowCross=False,
                                    tooltip={"placement": "bottom", "always_visible": False},
                                ),
                                html.Label("Google Trends"),
                                dcc.RangeSlider(
                                    id="trends-range",
                                    min=trends_min,
                                    max=trends_max,
                                    step=max((trends_max - trends_min) / 200, 0.1),
                                    value=[trends_min, trends_max],
                                    marks=trends_marks,
                                    allowCross=False,
                                    tooltip={"placement": "bottom", "always_visible": False},
                                ),
                            ],
                            className="panel",
                        ),
                    ],
                    className="sidebar",
                ),
                html.Main(
                    [
                        dcc.Graph(id="map", figure=_figure(initial_data), config={"displayModeBar": True, "scrollZoom": True}, className="map"),
                        html.Div(id="last-update", className="last-update"),
                    ],
                    className="map-wrap",
                ),
            ],
            className="workspace",
        ),
    ],
    className="app-shell",
)


app.index_string = """
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            * { box-sizing: border-box; }
            body { margin: 0; background: #07111f; color: #e5eefb; font-family: Inter, Segoe UI, Arial, sans-serif; }
            .app-shell { min-height: 100vh; background: #07111f; }
            .topbar { display: grid; grid-template-columns: minmax(320px, 1fr) minmax(420px, 0.85fr); gap: 24px; padding: 28px 32px 18px; border-bottom: 1px solid rgba(148,163,184,0.18); }
            .eyebrow { color: #38bdf8; text-transform: uppercase; font-size: 12px; letter-spacing: 0; font-weight: 800; margin-bottom: 8px; }
            h1 { margin: 0; font-size: 34px; line-height: 1.05; color: #f8fafc; }
            h2 { margin: 0 0 14px; font-size: 15px; color: #f8fafc; }
            p { margin: 10px 0 0; color: #9fb0c8; max-width: 760px; }
            .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; align-content: end; }
            .stat-card { background: #0d1b2e; border: 1px solid rgba(148,163,184,0.18); border-radius: 8px; padding: 14px; }
            .stat-title { color: #93a4bc; font-size: 12px; }
            .stat-value { color: #f8fafc; font-size: 26px; font-weight: 800; margin-top: 6px; }
            .stat-subtitle { color: #667991; font-size: 12px; margin-top: 4px; }
            .workspace { display: grid; grid-template-columns: 360px minmax(0, 1fr); min-height: calc(100vh - 138px); }
            .sidebar { padding: 18px; overflow-y: auto; border-right: 1px solid rgba(148,163,184,0.18); background: #0a1525; }
            .panel { background: #0d1b2e; border: 1px solid rgba(148,163,184,0.18); border-radius: 8px; padding: 16px; margin-bottom: 14px; }
            label { display: block; color: #c7d2e1; font-size: 12px; font-weight: 700; margin: 14px 0 8px; }
            input { width: 100%; height: 38px; border-radius: 6px; border: 1px solid #b9c6d8; background: #f8fafc; color: #0f172a; padding: 0 10px; font-weight: 700; }
            input::placeholder { color: #64748b; opacity: 1; }
            .primary-button { width: 100%; height: 42px; border: 0; border-radius: 6px; background: #38bdf8; color: #06111f; font-weight: 800; margin-top: 18px; cursor: pointer; }
            .primary-button:hover { background: #7dd3fc; }
            .status { min-height: 44px; margin-top: 12px; color: #b8c7da; font-size: 13px; line-height: 1.35; }
            .segmented label { display: inline-flex; margin: 0 8px 8px 0; color: #d8e2f0; }
            .map-wrap { position: relative; min-width: 0; }
            .map { height: calc(100vh - 138px); min-height: 660px; }
            .last-update { position: absolute; right: 16px; top: 16px; padding: 9px 12px; background: rgba(7,17,31,0.78); border: 1px solid rgba(148,163,184,0.24); border-radius: 8px; color: #d7e2f0; font-size: 12px; }
            .Select, .Select div, .Select span, .Select input,
            .dash-dropdown, .dash-dropdown div, .dash-dropdown span, .dash-dropdown input {
                color: #0f172a !important;
            }
            .Select-control, .Select__control {
                background: #f8fafc !important;
                border: 1px solid #b9c6d8 !important;
                box-shadow: none !important;
                min-height: 40px;
                border-radius: 6px !important;
            }
            .Select-control:hover, .Select__control:hover { border-color: #38bdf8 !important; }
            .Select-placeholder,
            .Select--single > .Select-control .Select-value,
            .Select__placeholder,
            .Select__single-value {
                color: #334155 !important;
                line-height: 38px !important;
                font-weight: 700 !important;
            }
            .Select-value-label { color: #0f172a !important; font-weight: 700 !important; }
            .Select-input input { color: #0f172a !important; font-weight: 700 !important; }
            .Select-menu-outer, .Select__menu {
                background: #f8fafc !important;
                border: 1px solid #b9c6d8 !important;
                box-shadow: 0 12px 28px rgba(0,0,0,0.35) !important;
                z-index: 1000 !important;
            }
            .Select-option, .Select__option {
                background: #f8fafc !important;
                color: #0f172a !important;
                font-weight: 700 !important;
            }
            .Select-option.is-focused, .Select__option--is-focused {
                background: #dbeafe !important;
                color: #0f172a !important;
            }
            .Select-option.is-selected, .Select__option--is-selected {
                background: #0e7490 !important;
                color: #ffffff !important;
            }
            .Select-multi-value-wrapper .Select-value, .Select__multi-value {
                background: #dbeafe !important;
                border: 1px solid #38bdf8 !important;
                color: #0f172a !important;
                border-radius: 4px !important;
            }
            .Select-multi-value-wrapper .Select-value-icon { border-right-color: #38bdf8 !important; color: #0f172a !important; }
            .Select-arrow { border-color: #0f172a transparent transparent !important; }
            .rc-slider { margin: 8px 4px 30px; }
            .rc-slider-mark { top: 20px; }
            .rc-slider-mark-text {
                color: #e2e8f0 !important;
                font-weight: 800;
                font-size: 11px;
                white-space: nowrap;
            }
            .rc-slider-rail { background-color: #334155; height: 6px; }
            .rc-slider-track { background-color: #38bdf8; height: 6px; }
            .rc-slider-handle {
                border: 3px solid #ffffff;
                background-color: #38bdf8;
                opacity: 1;
                width: 18px;
                height: 18px;
                margin-top: -6px;
                box-shadow: 0 0 0 2px rgba(56,189,248,0.35);
            }
            .rc-slider-dot { display: none; }
            .rc-slider-tooltip { display: none !important; }
            @media (max-width: 980px) {
                .topbar, .workspace { grid-template-columns: 1fr; }
                .stats-grid { grid-template-columns: 1fr; }
                .sidebar { border-right: 0; border-bottom: 1px solid rgba(148,163,184,0.18); }
                .map { height: 720px; }
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>{%config%}{%scripts%}{%renderer%}</footer>
    </body>
</html>
"""


@app.callback(
    Output("stream-status", "children"),
    Input("start-stream", "n_clicks"),
    State("event-count", "value"),
    State("event-delay", "value"),
    prevent_initial_call=True,
)
def start_stream(n_clicks: int, event_count: int, delay: float) -> str:
    if not n_clicks:
        return no_update
    return _start_random_events(int(event_count or 50), float(delay or 0.0))


@app.callback(
    Output("map", "figure"),
    Output("stat-visible", "children"),
    Output("stat-live", "children"),
    Output("stat-impacted", "children"),
    Output("stat-events", "children"),
    Output("last-update", "children"),
    Output("stream-status", "children", allow_duplicate=True),
    Input("refresh-interval", "n_intervals"),
    Input("departments", "value"),
    Input("source-mode", "value"),
    Input("type-mode", "value"),
    Input("score-range", "value"),
    Input("commune", "value"),
    Input("zone-mode", "value"),
    Input("price-range", "value"),
    Input("trends-range", "value"),
    prevent_initial_call="initial_duplicate",
)
def refresh_map(
    _tick: int,
    departments: list[str] | None,
    source_mode: str,
    type_mode: str,
    score_range: list[float],
    commune_query: str,
    zone_mode: str,
    price_range: list[float],
    trends_range: list[float],
) -> tuple[go.Figure, str, str, str, str, str, str]:
    data = load_clubs_dataframe()
    filtered = _filter_data(
        data,
        departments,
        source_mode,
        type_mode,
        score_range,
        commune_query or "",
        zone_mode,
        price_range,
        trends_range,
    )
    live_count = int((data["stream_source"] == "Streaming").sum()) if not data.empty else 0
    impacted_count = int(data["score_impacted"].sum()) if not data.empty else 0
    visible_impacted_count = int(filtered["score_impacted"].sum()) if not filtered.empty else 0
    state = _state_snapshot()
    event_text = f"{state['total']} ({state['created']} + / {state['updated']} mod. / {state['deleted']} suppr.)"
    timestamp = datetime.now().strftime("%H:%M:%S")
    status = state["last_message"]
    if state["running"]:
        status = f"{status} Events envoyes: {state['total']}"
    return (
        _figure(filtered),
        str(len(filtered)),
        str(live_count),
        f"{visible_impacted_count}/{impacted_count}",
        event_text,
        f"Derniere mise a jour: {timestamp}",
        status,
    )


def main() -> None:
    host = os.environ.get("DASH_HOST", "0.0.0.0")
    port = int(os.environ.get("DASH_PORT", "8050"))
    debug = os.environ.get("DASH_DEBUG", "0") == "1"
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
