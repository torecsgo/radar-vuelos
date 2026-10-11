import json
import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

KEYSPACE = "radar_flights"
DATA_DIR = os.getenv("ASTRA_DATA_DIR", "/app/data")
BUNDLE = os.getenv("ASTRA_BUNDLE", f"{DATA_DIR}/secure-connect-radar-flights.zip")
TZ = os.getenv("DASHBOARD_TZ", "Europe/Madrid")
COLORS = {"Flying": [0, 122, 255], "Taking Off": [40, 200, 90],
          "Landing": [255, 159, 10], "On Ground": [142, 142, 147]}
PALETTE = [[230, 57, 70], [29, 120, 200], [42, 157, 80], [244, 162, 97],
           [131, 56, 236], [0, 150, 160], [200, 80, 160], [90, 90, 90]]

st.set_page_config("Flight Radar", "✈️", layout="wide")


# ---------- Data access ----------
@st.cache_resource
def session():
    cid, secret = os.getenv("ASTRA_CLIENT_ID"), os.getenv("ASTRA_CLIENT_SECRET")
    if not cid:  # same file used by Spark
        with open(f"{DATA_DIR}/generated-token.json", encoding="utf-8") as f:
            t = json.load(f)
            cid, secret = t["clientId"], t["secret"]
    cluster = Cluster(cloud={"secure_connect_bundle": BUNDLE},
                      auth_provider=PlainTextAuthProvider(cid, secret))
    s = cluster.connect(KEYSPACE)
    s.row_factory = dict_factory
    return s


def query(cql, params=None):
    return pd.DataFrame(list(session().execute(cql, params or [])))


@st.cache_data(ttl=600)
def load_planes():
    df = query("SELECT icao24, callsign, origin_country FROM planes")
    return df.drop_duplicates("icao24", keep="last") if not df.empty else df


@st.cache_data(ttl=30)
def load_recent(minutes):
    """Last known position of each aircraft within the time window."""
    since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=minutes)
    try:
        df = query("SELECT * FROM flights WHERE time_position_date > %s ALLOW FILTERING", [since])
    except Exception:
        df = query("SELECT * FROM flights LIMIT 20000")
        if not df.empty:
            df = df[df.time_position_date > since]
    if df.empty:
        return df
    df = df.dropna(subset=["latitude", "longitude"])
    return df.sort_values("time_position_date").drop_duplicates("icao24", keep="last")


@st.cache_data(ttl=60)
def load_history(icao24):
    df = query("SELECT * FROM flights WHERE icao24 = %s", [icao24])
    return df.sort_values("time_position_date") if not df.empty else df


# ---------- Logic ----------
def current_trip(h, gap_min):
    """Current trip = points since the last takeoff or since the last long gap without data."""
    h = h.dropna(subset=["latitude", "longitude"]).sort_values("time_position_date").reset_index(drop=True)
    if h.empty:
        return h
    g = h.flying_status.eq("On Ground")
    new = (h.time_position_date.diff() > pd.Timedelta(minutes=gap_min)) | (g & ~g.shift(-1, fill_value=True))
    ids = new.cumsum()
    return h[ids == ids.iloc[-1]]


def dist_km(d):
    la, lo = np.radians(d.latitude.values), np.radians(d.longitude.values)
    a = np.sin(np.diff(la) / 2) ** 2 + np.cos(la[:-1]) * np.cos(la[1:]) * np.sin(np.diff(lo) / 2) ** 2
    return float((12742 * np.arcsin(np.sqrt(a))).sum())


def local(s):
    return s.dt.tz_localize("UTC").dt.tz_convert(TZ)


# ---------- Sidebar ----------
try:
    planes = load_planes()
except Exception as e:
    st.error(f"Can't connect to Cassandra: {e}")
    st.stop()

sb = st.sidebar
sb.title("✈️ Flight Radar")
mode = sb.radio("View", ["📡 Live radar", "🧭 Current trip"])
minutes = sb.slider("\"Live\" window (min)", 5, 120, 20,
                    help="The producer polls OpenSky every ~400 s, so a window under 10 min may leave the map empty.")
refresh = sb.selectbox("Auto-refresh", ["Off", "15 s", "30 s", "60 s"], index=2)
if sb.button("Refresh now"):
    st.cache_data.clear()
sb.divider()
search = sb.text_input("Search callsign or ICAO24")
countries = sb.multiselect("Origin country", sorted(planes.origin_country.dropna().unique()) if not planes.empty else [])
statuses = sb.multiselect("Status", list(COLORS), default=list(COLORS))
alt = sb.slider("Altitude (m)", 0, 13000, (0, 13000), step=500)
gap = sb.number_input("Max gap within a trip (min)", 15, 720, 120, step=15) if mode.startswith("🧭") else 120


# ---------- Content ----------
def render():
    cur = load_recent(minutes)
    if cur.empty:
        st.warning("No data in the selected window. Widen it or check that the producer and Spark are running.")
        return
    df = cur.merge(planes[["icao24", "origin_country"]], on="icao24", how="left")
    df["callsign"] = df.callsign.fillna("—")
    q = search.strip().lower()
    if q:
        df = df[df.callsign.str.lower().str.contains(q, regex=False) | df.icao24.str.contains(q, regex=False)]
    if countries:
        df = df[df.origin_country.isin(countries)]
    df = df[df.flying_status.isin(statuses) & df.baro_altitude.fillna(0).between(*alt)].copy()

    age = (datetime.now(timezone.utc).replace(tzinfo=None) - cur.time_position_date.max()).total_seconds() / 60
    k = st.columns(4)
    k[0].metric("Aircraft", len(df))
    k[1].metric("Average speed", f"{df.velocity_km_h.mean():.0f} km/h" if len(df) else "—")
    k[2].metric("Average altitude", f"{df.baro_altitude.mean():.0f} m" if len(df) else "—")
    k[3].metric("Latest data", f"{age:.0f} min ago")
    if df.empty:
        st.info("No aircraft match the filters.")
        return

    df["color"] = df.flying_status.map(COLORS)
    df["alt"] = df.baro_altitude.fillna(0).round().astype(int)
    df["vel"] = df.velocity_km_h.fillna(0).round().astype(int)
    df["time"] = local(df.time_position_date).dt.strftime("%H:%M:%S")
    view = pdk.ViewState(latitude=float(df.latitude.mean()), longitude=float(df.longitude.mean()), zoom=3.4)
    tip = {"html": "<b>{name}</b><br/>{sub}"}

    if mode.startswith("📡"):
        d = df[["longitude", "latitude", "color", "callsign", "icao24", "alt", "vel", "flying_status", "time"]].copy()
        d["name"] = d.callsign
        d["sub"] = d.apply(lambda r: f"{r.icao24} · {r.flying_status}<br/>{r.alt} m · {r.vel} km/h · {r.time}", axis=1)
        layer = pdk.Layer("ScatterplotLayer", d, get_position=["longitude", "latitude"], get_fill_color="color",
                          get_radius=5000, radius_min_pixels=4, radius_max_pixels=10, pickable=True)
        st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view, tooltip=tip, map_style="light"),
                        use_container_width=True)
        st.dataframe(df[["callsign", "icao24", "origin_country", "flying_status", "alt", "vel", "time"]]
                     .rename(columns={"alt": "altitude_m", "vel": "km_h", "time": "last_update"}),
                     use_container_width=True, hide_index=True)
        return

    opts = dict(zip(df.callsign + " · " + df.icao24, df.icao24))
    sel = st.multiselect("Aircraft to track", list(opts), default=list(opts)[:1], max_selections=8, key="sel")
    paths, ends, rows, series = [], [], [], {}
    for i, label in enumerate(sel):
        t = current_trip(load_history(opts[label]), gap)
        if t.empty:
            continue
        col = PALETTE[i % len(PALETTE)]
        paths.append({"path": t[["longitude", "latitude"]].values.tolist(), "color": col, "name": label, "sub": "current trip"})
        last = t.iloc[-1]
        ends.append({"longitude": float(last.longitude), "latitude": float(last.latitude), "color": col,
                     "name": label, "sub": f"{last.flying_status} · {last.baro_altitude:.0f} m"})
        dur = t.time_position_date.iloc[-1] - t.time_position_date.iloc[0]
        rows.append({"aircraft": label, "points": len(t), "distance_km": round(dist_km(t)),
                     "duration": str(dur).split(".")[0], "max_alt_m": round(t.baro_altitude.max()),
                     "avg_speed_kmh": round(t.velocity_km_h.mean()), "current_status": last.flying_status})
        series[label] = t.assign(h=local(t.time_position_date)).set_index("h").baro_altitude
    if not paths:
        st.info("Select at least one aircraft with history.")
        return
    pts = np.array([p for x in paths for p in x["path"]])
    view = pdk.ViewState(latitude=float(pts[:, 1].mean()), longitude=float(pts[:, 0].mean()), zoom=4.5)
    layers = [pdk.Layer("PathLayer", paths, get_path="path", get_color="color", width_min_pixels=3, pickable=True),
              pdk.Layer("ScatterplotLayer", ends, get_position=["longitude", "latitude"], get_fill_color="color",
                        get_radius=9000, radius_min_pixels=7, pickable=True)]
    st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view, tooltip=tip, map_style="light"),
                    use_container_width=True)
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    st.caption("Altitude (m) along the trip")
    st.line_chart(pd.DataFrame(series))


every = None if refresh == "Off" else int(refresh.split()[0])
st.fragment(run_every=every)(render)()
