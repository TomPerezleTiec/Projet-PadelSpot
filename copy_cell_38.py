# Carte interactive Plotly des clubs de padel (CSV unique) - version stable

from pathlib import Path
import math
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import ipywidgets as widgets
from IPython.display import display, clear_output

CSV_FILENAME = "part-00000-fa44d60d-0c5b-428a-b1ed-a24b693f9e69-c000.csv"


def resolve_csv_path(filename: str) -> Path:
    candidates = []
    local_expected = Path("data") / "dash_ready" / "dash_clubs.csv" / filename
    if local_expected.exists():
        return local_expected.resolve()

    for root in [Path.cwd(), Path("/workspaces"), Path("/workspace"), Path("/")]:
        if root.exists():
            try:
                candidates.append(next(root.rglob(filename)))
            except StopIteration:
                pass
            except Exception:
                pass
        if candidates:
            break

    if not candidates:
        raise FileNotFoundError(
            f"Fichier introuvable: {filename}. "
            "Place-le dans data/dash_ready/dash_clubs.csv/ ou adapte le chemin."
        )
    return candidates[0].resolve()


DEPT_INFO = {
    "01": ("Ain", "Auvergne-Rhone-Alpes"), "02": ("Aisne", "Hauts-de-France"),
    "03": ("Allier", "Auvergne-Rhone-Alpes"), "04": ("Alpes-de-Haute-Provence", "Provence-Alpes-Cote d'Azur"),
    "05": ("Hautes-Alpes", "Provence-Alpes-Cote d'Azur"), "06": ("Alpes-Maritimes", "Provence-Alpes-Cote d'Azur"),
    "07": ("Ardeche", "Auvergne-Rhone-Alpes"), "08": ("Ardennes", "Grand Est"),
    "09": ("Ariege", "Occitanie"), "10": ("Aube", "Grand Est"), "11": ("Aude", "Occitanie"),
    "12": ("Aveyron", "Occitanie"), "13": ("Bouches-du-Rhone", "Provence-Alpes-Cote d'Azur"),
    "14": ("Calvados", "Normandie"), "15": ("Cantal", "Auvergne-Rhone-Alpes"),
    "16": ("Charente", "Nouvelle-Aquitaine"), "17": ("Charente-Maritime", "Nouvelle-Aquitaine"),
    "18": ("Cher", "Centre-Val de Loire"), "19": ("Correze", "Nouvelle-Aquitaine"),
    "21": ("Cote-d'Or", "Bourgogne-Franche-Comte"), "22": ("Cotes-d'Armor", "Bretagne"),
    "23": ("Creuse", "Nouvelle-Aquitaine"), "24": ("Dordogne", "Nouvelle-Aquitaine"),
    "25": ("Doubs", "Bourgogne-Franche-Comte"), "26": ("Drome", "Auvergne-Rhone-Alpes"),
    "27": ("Eure", "Normandie"), "28": ("Eure-et-Loir", "Centre-Val de Loire"),
    "29": ("Finistere", "Bretagne"), "2A": ("Corse-du-Sud", "Corse"), "2B": ("Haute-Corse", "Corse"),
    "30": ("Gard", "Occitanie"), "31": ("Haute-Garonne", "Occitanie"), "32": ("Gers", "Occitanie"),
    "33": ("Gironde", "Nouvelle-Aquitaine"), "34": ("Herault", "Occitanie"), "35": ("Ille-et-Vilaine", "Bretagne"),
    "36": ("Indre", "Centre-Val de Loire"), "37": ("Indre-et-Loire", "Centre-Val de Loire"),
    "38": ("Isere", "Auvergne-Rhone-Alpes"), "39": ("Jura", "Bourgogne-Franche-Comte"),
    "40": ("Landes", "Nouvelle-Aquitaine"), "41": ("Loir-et-Cher", "Centre-Val de Loire"),
    "42": ("Loire", "Auvergne-Rhone-Alpes"), "43": ("Haute-Loire", "Auvergne-Rhone-Alpes"),
    "44": ("Loire-Atlantique", "Pays de la Loire"), "45": ("Loiret", "Centre-Val de Loire"),
    "46": ("Lot", "Occitanie"), "47": ("Lot-et-Garonne", "Nouvelle-Aquitaine"),
    "48": ("Lozere", "Occitanie"), "49": ("Maine-et-Loire", "Pays de la Loire"),
    "50": ("Manche", "Normandie"), "51": ("Marne", "Grand Est"), "52": ("Haute-Marne", "Grand Est"),
    "53": ("Mayenne", "Pays de la Loire"), "54": ("Meurthe-et-Moselle", "Grand Est"),
    "55": ("Meuse", "Grand Est"), "56": ("Morbihan", "Bretagne"), "57": ("Moselle", "Grand Est"),
    "58": ("Nievre", "Bourgogne-Franche-Comte"), "59": ("Nord", "Hauts-de-France"),
    "60": ("Oise", "Hauts-de-France"), "61": ("Orne", "Normandie"), "62": ("Pas-de-Calais", "Hauts-de-France"),
    "63": ("Puy-de-Dome", "Auvergne-Rhone-Alpes"), "64": ("Pyrenees-Atlantiques", "Nouvelle-Aquitaine"),
    "65": ("Hautes-Pyrenees", "Occitanie"), "66": ("Pyrenees-Orientales", "Occitanie"),
    "67": ("Bas-Rhin", "Grand Est"), "68": ("Haut-Rhin", "Grand Est"), "69": ("Rhone", "Auvergne-Rhone-Alpes"),
    "70": ("Haute-Saone", "Bourgogne-Franche-Comte"), "71": ("Saone-et-Loire", "Bourgogne-Franche-Comte"),
    "72": ("Sarthe", "Pays de la Loire"), "73": ("Savoie", "Auvergne-Rhone-Alpes"),
    "74": ("Haute-Savoie", "Auvergne-Rhone-Alpes"), "75": ("Paris", "Ile-de-France"),
    "76": ("Seine-Maritime", "Normandie"), "77": ("Seine-et-Marne", "Ile-de-France"),
    "78": ("Yvelines", "Ile-de-France"), "79": ("Deux-Sevres", "Nouvelle-Aquitaine"),
    "80": ("Somme", "Hauts-de-France"), "81": ("Tarn", "Occitanie"), "82": ("Tarn-et-Garonne", "Occitanie"),
    "83": ("Var", "Provence-Alpes-Cote d'Azur"), "84": ("Vaucluse", "Provence-Alpes-Cote d'Azur"),
    "85": ("Vendee", "Pays de la Loire"), "86": ("Vienne", "Nouvelle-Aquitaine"),
    "87": ("Haute-Vienne", "Nouvelle-Aquitaine"), "88": ("Vosges", "Grand Est"),
    "89": ("Yonne", "Bourgogne-Franche-Comte"), "90": ("Territoire de Belfort", "Bourgogne-Franche-Comte"),
    "91": ("Essonne", "Ile-de-France"), "92": ("Hauts-de-Seine", "Ile-de-France"),
    "93": ("Seine-Saint-Denis", "Ile-de-France"), "94": ("Val-de-Marne", "Ile-de-France"),
    "95": ("Val-d'Oise", "Ile-de-France"),
}

REGION_ORDER = [
    "Auvergne-Rhone-Alpes", "Bourgogne-Franche-Comte", "Bretagne", "Centre-Val de Loire", "Corse",
    "Grand Est", "Hauts-de-France", "Ile-de-France", "Normandie", "Nouvelle-Aquitaine", "Occitanie",
    "Pays de la Loire", "Provence-Alpes-Cote d'Azur", "Autres"
]


def _dept_meta(code):
    code_s = str(code)
    if code_s in DEPT_INFO:
        return DEPT_INFO[code_s]
    return f"Departement {code_s}", "Autres"


def _zoom_from_bounds(lat_min, lat_max, lon_min, lon_max):
    lat_span = max(0.12, float(lat_max - lat_min) * 1.18)
    lon_span = max(0.12, float(lon_max - lon_min) * 1.18)
    z_lon = math.log2(360.0 / lon_span)
    z_lat = math.log2(170.0 / lat_span)
    return max(3.0, min(12.0, min(z_lon, z_lat) - 0.25))


CSV_PATH = resolve_csv_path(CSV_FILENAME)
print(f"CSV utilise: {CSV_PATH}")

clubs = pd.read_csv(CSV_PATH)
required_cols = [
    "nom", "type", "commune", "latitude", "longitude", "nombre_de_courts", "source", "source_principale",
    "departement_code", "score_zone_implantation", "prix_median_m2_zone", "ind_snv_zone",
    "indice_demande_trends_zone", "part_cible_padel_zone", "zone_saturee"
]
for c in required_cols:
    if c not in clubs.columns:
        clubs[c] = np.nan

clubs["latitude"] = pd.to_numeric(clubs["latitude"], errors="coerce")
clubs["longitude"] = pd.to_numeric(clubs["longitude"], errors="coerce")
clubs["nombre_de_courts"] = pd.to_numeric(clubs["nombre_de_courts"], errors="coerce")
clubs["score_zone_implantation"] = pd.to_numeric(clubs["score_zone_implantation"], errors="coerce")

mask_padel = (
    clubs["nom"].fillna("").str.contains("padel", case=False, na=False)
    | clubs["type"].fillna("").str.contains("padel", case=False, na=False)
)
mask_metro = (
    clubs["latitude"].between(41.0, 51.5, inclusive="both")
    & clubs["longitude"].between(-5.5, 10.5, inclusive="both")
)

base = clubs.loc[mask_padel & mask_metro].copy()
if base.empty:
    raise ValueError("Aucun club padel en France metropolitaine avec ce CSV.")

base["source_principale_bool"] = (
    base["source_principale"]
    .astype(str).str.strip().str.lower()
    .map({"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False})
    .fillna(False)
)
base["source"] = base["source"].fillna("N/A").astype(str)
base["type"] = base["type"].fillna("N/A").astype(str)

all_deps = sorted(base["departement_code"].dropna().astype(str).unique().tolist())
all_actors = sorted(base["source"].dropna().astype(str).unique().tolist())
all_types = sorted(base["type"].dropna().astype(str).unique().tolist())

region_to_options = {}
for dep in all_deps:
    dep_name, region_name = _dept_meta(dep)
    region_to_options.setdefault(region_name, []).append((f"{dep_name} - {dep}", dep))
for r in region_to_options:
    region_to_options[r] = sorted(region_to_options[r], key=lambda x: x[0])
ordered_regions = [r for r in REGION_ORDER if r in region_to_options] + sorted(
    [r for r in region_to_options if r not in REGION_ORDER]
)

dept_selectors = {}
children = []
for region in ordered_regions:
    w_region = widgets.SelectMultiple(
        options=region_to_options[region], value=(), description="",
        layout=widgets.Layout(width="350px", height="150px")
    )
    dept_selectors[region] = w_region
    children.append(w_region)

w_deps_by_region = widgets.Accordion(children=children, selected_index=0)
for i, region in enumerate(ordered_regions):
    w_deps_by_region.set_title(i, region)

w_reset_deps = widgets.Button(description="Vue France (reset dep)", layout=widgets.Layout(width="220px"))

score_min = float(np.nanmin(base["score_zone_implantation"]))
score_max = float(np.nanmax(base["score_zone_implantation"]))
global_min = float(base["score_zone_implantation"].min())
global_max = float(base["score_zone_implantation"].max())
if np.isfinite(global_min) and np.isfinite(global_max) and global_min == global_max:
    eps = max(abs(global_min) * 0.01, 1e-6)
    global_min -= eps
    global_max += eps

w_actors = widgets.SelectMultiple(
    options=all_actors, value=(), description="Acteurs:", layout=widgets.Layout(width="270px", height="150px")
)
w_types = widgets.SelectMultiple(
    options=all_types, value=(), description="Type:", layout=widgets.Layout(width="320px", height="150px")
)
w_score = widgets.FloatRangeSlider(
    value=[score_min, score_max], min=score_min, max=score_max,
    step=max((score_max - score_min) / 300.0, 0.0001), description="Score:",
    continuous_update=False, readout_format=".3f", layout=widgets.Layout(width="500px")
)
w_source = widgets.Checkbox(value=False, description="Source principale uniquement")
w_mode = widgets.ToggleButtons(
    options=[("Global", "global"), ("Local", "local")], value="global", description="Mode couleur:"
)
visible_count = widgets.HTML()

FRANCE_CENTER_LAT = 46.6
FRANCE_CENTER_LON = 2.3
FRANCE_ZOOM = 5.0
view_state = {"center_lat": FRANCE_CENTER_LAT, "center_lon": FRANCE_CENTER_LON, "zoom": FRANCE_ZOOM}
ui_state = {"suspend": False}

map_out = widgets.Output(layout=widgets.Layout(width="100%", height="1050px"))


def _selected_deps():
    selected = []
    for region in ordered_regions:
        selected.extend(list(dept_selectors[region].value))
    return set(selected)


def _apply_filters():
    df = base.copy()

    sel_deps = _selected_deps()
    if sel_deps:
        df = df[df["departement_code"].astype(str).isin(sel_deps)]

    sel_actors = set(w_actors.value)
    if sel_actors:
        df = df[df["source"].astype(str).isin(sel_actors)]

    sel_types = set(w_types.value)
    if sel_types:
        df = df[df["type"].astype(str).isin(sel_types)]

    smin, smax = w_score.value
    df = df[df["score_zone_implantation"].between(smin, smax, inclusive="both")]

    if w_source.value:
        df = df[df["source_principale_bool"]]

    return df


def _compute_sizes(df):
    courts = pd.to_numeric(df["nombre_de_courts"], errors="coerce")
    sizes = np.where(courts.isna(), 7.0, 4.0 + courts.clip(lower=1) * 1.8)
    return np.clip(sizes, 6.0, 28.0)


def _get_color_range(df):
    if w_mode.value == "global":
        return global_min, global_max

    cmin = float(df["score_zone_implantation"].min()) if not df.empty else global_min
    cmax = float(df["score_zone_implantation"].max()) if not df.empty else global_max
    if not np.isfinite(cmin) or not np.isfinite(cmax):
        cmin, cmax = global_min, global_max
    elif cmin == cmax:
        eps = max(abs(cmin) * 0.01, 1e-6)
        cmin -= eps
        cmax += eps
    return cmin, cmax


def _auto_zoom_to_df(df):
    if df.empty:
        return
    lat_min = float(df["latitude"].min())
    lat_max = float(df["latitude"].max())
    lon_min = float(df["longitude"].min())
    lon_max = float(df["longitude"].max())
    view_state["center_lat"] = (lat_min + lat_max) / 2.0
    view_state["center_lon"] = (lon_min + lon_max) / 2.0
    view_state["zoom"] = _zoom_from_bounds(lat_min, lat_max, lon_min, lon_max)


def _set_view_france():
    view_state["center_lat"] = FRANCE_CENTER_LAT
    view_state["center_lon"] = FRANCE_CENTER_LON
    view_state["zoom"] = FRANCE_ZOOM


def _draw_map(df):
    cmin, cmax = _get_color_range(df)
    fig = go.Figure()

    if not df.empty:
        sizes = _compute_sizes(df)
        custom = np.stack([
            df["nom"].fillna("N/A").astype(str),
            df["commune"].fillna("N/A").astype(str),
            df["departement_code"].fillna("N/A").astype(str),
            df["source"].fillna("N/A").astype(str),
            df["type"].fillna("N/A").astype(str),
            df["score_zone_implantation"].fillna(np.nan),
            df["prix_median_m2_zone"].fillna(np.nan),
            df["ind_snv_zone"].fillna(np.nan),
            df["indice_demande_trends_zone"].fillna(np.nan),
            df["part_cible_padel_zone"].fillna(np.nan),
            df["zone_saturee"].fillna("N/A").astype(str),
        ], axis=-1)

        fig.add_trace(go.Scattermap(
            lat=df["latitude"], lon=df["longitude"], mode="markers", hoverinfo="skip",
            marker=dict(size=(np.array(sizes) + 1.2).tolist(), color="rgba(20,20,20,0.55)", opacity=0.8),
            showlegend=False,
        ))

        fig.add_trace(go.Scattermap(
            lat=df["latitude"], lon=df["longitude"], mode="markers", customdata=custom,
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Commune: %{customdata[1]}<br>"
                "Departement: %{customdata[2]}<br>"
                "Acteur: %{customdata[3]}<br>"
                "Type: %{customdata[4]}<br>"
                "Score implantation: %{customdata[5]:.3f}<br>"
                "Prix median m2: %{customdata[6]:.0f}<br>"
                "ind_snv_zone: %{customdata[7]:.0f}<br>"
                "indice_demande_trends_zone: %{customdata[8]:.1f}<br>"
                "part_cible_padel_zone: %{customdata[9]:.3f}<br>"
                "zone_saturee: %{customdata[10]}"
                "<extra></extra>"
            ),
            marker=dict(
                size=np.array(sizes).tolist(), color=df["score_zone_implantation"].to_list(),
                colorscale="Viridis", cmin=cmin, cmax=cmax, opacity=0.8,
                colorbar=dict(title="Couleur: Score d'implantation")
            ),
            showlegend=False,
        ))

    fig.update_layout(
        title=f"Clubs de Padel en France ({len(df)} clubs visibles)",
        template="plotly_white",
        height=1000,
        margin=dict(l=0, r=0, t=70, b=0),
        map=dict(
            style="open-street-map",
            center=dict(lat=view_state["center_lat"], lon=view_state["center_lon"]),
            zoom=view_state["zoom"],
        ),
    )

    with map_out:
        clear_output(wait=True)
        display(fig)


def _render(*_):
    if ui_state["suspend"]:
        return
    df = _apply_filters()
    visible_count.value = f"<b>{len(df)} clubs visibles</b>"
    _draw_map(df)


def _on_deps_change(_change=None):
    if ui_state["suspend"]:
        return

    selected = _selected_deps()
    if selected:
        if w_mode.value != "local":
            w_mode.value = "local"
        _auto_zoom_to_df(_apply_filters())
    else:
        w_mode.value = "global"
        _set_view_france()
    _render()


def _on_reset(_btn):
    ui_state["suspend"] = True
    try:
        for region in ordered_regions:
            dept_selectors[region].value = ()
    finally:
        ui_state["suspend"] = False
    w_mode.value = "global"
    _set_view_france()
    _render()


for region in ordered_regions:
    dept_selectors[region].observe(_on_deps_change, names="value")
w_reset_deps.on_click(_on_reset)
for w in [w_actors, w_types, w_score, w_source, w_mode]:
    w.observe(_render, names="value")

controls = widgets.VBox([
    widgets.HBox([widgets.VBox([w_deps_by_region, w_reset_deps]), w_actors, w_types]),
    widgets.HBox([w_score, w_source, w_mode, visible_count]),
])

display(controls, map_out)
_render()
