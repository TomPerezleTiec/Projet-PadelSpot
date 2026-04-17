# Reagencement UI: departements a gauche, carte a droite, autres filtres en bas

import ipywidgets as widgets
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from IPython.display import display, clear_output

required = [
    "base", "map_out", "_compute_sizes", "_get_color_range", "view_state", "ui_state", "_auto_zoom_to_df", "_set_view_france"
]
missing = [name for name in required if name not in globals()]
if missing:
    raise RuntimeError("Execute d'abord la cellule carte, variables manquantes: " + ", ".join(missing))

# ---------- Departements par regions avec cases a cocher ----------
def _local_dept_meta(code):
    code_s = str(code)
    if "DEPT_INFO" in globals() and code_s in DEPT_INFO:
        name, region = DEPT_INFO[code_s]
        return name, region
    return f"Departement {code_s}", "Autres"

all_deps = sorted(base["departement_code"].dropna().astype(str).unique().tolist())
region_to_deps = {}
for dep in all_deps:
    name, region = _local_dept_meta(dep)
    region_to_deps.setdefault(region, []).append((dep, f"{name} - {dep}"))
for r in region_to_deps:
    region_to_deps[r] = sorted(region_to_deps[r], key=lambda x: x[1])

if "REGION_ORDER" in globals():
    ordered_regions = [r for r in REGION_ORDER if r in region_to_deps]
    ordered_regions += sorted([r for r in region_to_deps if r not in ordered_regions])
else:
    ordered_regions = sorted(region_to_deps.keys())

dep_checkboxes = {}
accordion_children = []
for region in ordered_regions:
    region_boxes = []
    for dep_code, dep_label in region_to_deps[region]:
        cb = widgets.Checkbox(value=False, description=dep_label, indent=False)
        cb.layout = widgets.Layout(width="100%")
        dep_checkboxes[dep_code] = cb
        region_boxes.append(cb)

    region_panel = widgets.VBox(
        region_boxes,
        layout=widgets.Layout(height="170px", overflow_y="auto", overflow_x="hidden", padding="4px 6px")
    )
    accordion_children.append(region_panel)

w_deps_by_region = widgets.Accordion(children=accordion_children, selected_index=0, layout=widgets.Layout(width="100%"))
for i, region in enumerate(ordered_regions):
    w_deps_by_region.set_title(i, region)

w_reset_deps = widgets.Button(
    description="Vue France (reset dep)",
    icon="refresh",
    layout=widgets.Layout(width="100%")
)

# CSS: supprime le scroll horizontal et force le retour a la ligne sur les labels longs
w_dept_css = widgets.HTML("""
<style>
.jp-OutputArea .widget-accordion .widget-box {
  overflow-x: hidden !important;
}
.jp-OutputArea .widget-accordion .widget-inline-hbox label {
  white-space: normal !important;
  word-break: break-word !important;
}
</style>
""")

# Marqueur visuel pour confirmer la MAJ de la cellule
w_ui_stamp = widgets.HTML("<div style='font-size:12px; color:#4b5563; margin-bottom:2px;'>Mise a jour UI activee</div>")


def _selected_deps():
    return {code for code, cb in dep_checkboxes.items() if cb.value}


# ---------- Autres filtres ----------
price_series = pd.to_numeric(base["prix_median_m2_zone"], errors="coerce")
price_min = float(np.nanmin(price_series)) if np.isfinite(np.nanmin(price_series)) else 0.0
price_max = float(np.nanmax(price_series)) if np.isfinite(np.nanmax(price_series)) else 1000.0
if price_min == price_max:
    price_max = price_min + 1.0

trends_series = pd.to_numeric(base["indice_demande_trends_zone"], errors="coerce")
trends_min = float(np.nanmin(trends_series)) if np.isfinite(np.nanmin(trends_series)) else 0.0
trends_max = float(np.nanmax(trends_series)) if np.isfinite(np.nanmax(trends_series)) else 100.0
if trends_min == trends_max:
    trends_max = trends_min + 1.0

score_series = pd.to_numeric(base["score_zone_implantation"], errors="coerce")
score_min = float(np.nanmin(score_series))
score_max = float(np.nanmax(score_series))

w_type = widgets.Dropdown(
    options=[
        ("Tout", "all"),
        ("Club de padel", "club_padel"),
        ("Club de tennis/padel", "club_tennis_padel"),
        ("Piste de padel", "piste_padel"),
    ],
    value="all",
    description="Type:",
    layout=widgets.Layout(width="100%")
)

w_score = widgets.FloatRangeSlider(
    value=[score_min, score_max],
    min=score_min,
    max=score_max,
    step=max((score_max - score_min) / 300.0, 0.0001),
    description="Score:",
    continuous_update=False,
    readout_format=".3f",
    layout=widgets.Layout(width="100%")
)

w_mode = widgets.ToggleButtons(
    options=[("Global", "global"), ("Local", "local")],
    value="global",
    description="Mode:"
)

w_commune = widgets.Text(
    value="",
    description="Commune:",
    placeholder="ex: Paris, Lyon...",
    layout=widgets.Layout(width="100%")
)

w_zone_saturee = widgets.Dropdown(
    options=[("Toutes", "all"), ("Saturee", "true"), ("Non saturee", "false")],
    value="all",
    description="Zone:",
    layout=widgets.Layout(width="100%")
)

w_price = widgets.FloatRangeSlider(
    value=[price_min, price_max],
    min=price_min,
    max=price_max,
    step=max((price_max - price_min) / 300.0, 0.1),
    description="Prix m2:",
    continuous_update=False,
    readout_format=".1f",
    layout=widgets.Layout(width="100%")
)

w_trends = widgets.FloatRangeSlider(
    value=[trends_min, trends_max],
    min=trends_min,
    max=trends_max,
    step=max((trends_max - trends_min) / 300.0, 0.1),
    description="Trends:",
    continuous_update=False,
    readout_format=".1f",
    layout=widgets.Layout(width="100%")
)

visible_count = widgets.HTML(
    value="<div style='text-align:center; font-weight:700; font-size:14px; margin-top:4px;'>0 club visible</div>"
)


def _type_mask(df, selected_type):
    txt_type = df["type"].fillna("").str.lower()
    txt_nom = df["nom"].fillna("").str.lower()

    if selected_type == "all":
        return pd.Series([True] * len(df), index=df.index)
    if selected_type == "club_tennis_padel":
        return (txt_type.str.contains("tennis") & txt_type.str.contains("padel")) | (
            txt_nom.str.contains("tennis") & txt_nom.str.contains("padel")
        )
    if selected_type == "piste_padel":
        return (txt_type.str.contains("piste") & txt_type.str.contains("padel")) | (
            txt_nom.str.contains("piste") & txt_nom.str.contains("padel")
        )
    # club_padel
    return (
        (txt_type.str.contains("club") & txt_type.str.contains("padel") & ~txt_type.str.contains("tennis"))
        | (txt_nom.str.contains("club") & txt_nom.str.contains("padel") & ~txt_nom.str.contains("tennis"))
    )


def _apply_filters():
    df = base.copy()

    sel_deps = _selected_deps()
    if sel_deps:
        df = df[df["departement_code"].astype(str).isin(sel_deps)]

    df = df[_type_mask(df, w_type.value)]

    smin, smax = w_score.value
    df = df[df["score_zone_implantation"].between(smin, smax, inclusive="both")]

    commune_q = w_commune.value.strip().lower()
    if commune_q:
        df = df[df["commune"].fillna("").str.lower().str.contains(commune_q, na=False)]

    zone_mode = w_zone_saturee.value
    if zone_mode != "all":
        zs = df["zone_saturee"].astype(str).str.strip().str.lower()
        if zone_mode == "true":
            df = df[zs.isin(["true", "1", "yes", "oui"])]
        else:
            df = df[zs.isin(["false", "0", "no", "non"])]

    pmin, pmax = w_price.value
    prices = pd.to_numeric(df["prix_median_m2_zone"], errors="coerce")
    df = df[prices.between(pmin, pmax, inclusive="both") | prices.isna()]

    tmin, tmax = w_trends.value
    trends = pd.to_numeric(df["indice_demande_trends_zone"], errors="coerce")
    df = df[trends.between(tmin, tmax, inclusive="both") | trends.isna()]

    return df


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
                size=np.array(sizes).tolist(),
                color=df["score_zone_implantation"].to_list(),
                colorscale="Viridis",
                cmin=cmin,
                cmax=cmax,
                opacity=0.8,
                colorbar=dict(title="Score", thickness=9, len=0.56, x=0.985, xanchor="left", outlinewidth=0),
            ),
            showlegend=False,
        ))

    fig.update_layout(
        title=f"Clubs de Padel en France ({len(df)} clubs visibles)",
        template="plotly_white",
        height=820,
        margin=dict(l=0, r=0, t=60, b=0),
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
    suffix = "club visible" if len(df) == 1 else "clubs visibles"
    visible_count.value = f"<div style='text-align:center; font-weight:700; font-size:14px; margin-top:4px;'>{len(df)} {suffix}</div>"
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
        for cb in dep_checkboxes.values():
            cb.value = False
    finally:
        ui_state["suspend"] = False
    w_mode.value = "global"
    _set_view_france()
    _render()


# ---------- Layout harmonise ----------
map_out.layout = widgets.Layout(height="840px", min_width="770px", flex="1 1 0%", width="1px")

left_panel = widgets.VBox(
    [w_dept_css, w_ui_stamp, widgets.HTML("<b>Departements (multi-selection)</b>"), w_deps_by_region, w_reset_deps],
    layout=widgets.Layout(
        width="360px",
        min_width="360px",
        flex="0 0 360px",
        border="1px solid #dfe3e8",
        padding="12px",
        border_radius="10px",
        gap="10px",
        overflow_x="hidden"
    )
)

map_panel = widgets.VBox(
    [map_out],
    layout=widgets.Layout(flex="1 1 auto", width="auto", min_width="770px")
)

top_row = widgets.HBox(
    [left_panel, map_panel],
    layout=widgets.Layout(width="100%", align_items="flex-start", gap="12px")
)

card_layout = widgets.Layout(
    border="1px solid #dfe3e8",
    padding="12px",
    border_radius="10px",
    width="calc(25% - 9px)",
    min_width="270px",
    gap="8px"
)

score_card = widgets.VBox(
    [
        widgets.HTML("<b>Score d'implantation</b>"),
        w_score,
        widgets.HBox([w_mode], layout=widgets.Layout(justify_content="center")),
        visible_count,
    ],
    layout=card_layout
)

card_primary = widgets.VBox(
    [widgets.HTML("<b>Type</b>"), w_type],
    layout=card_layout
)

card_geo = widgets.VBox(
    [widgets.HTML("<b>Filtres geographiques</b>"), w_commune, w_zone_saturee],
    layout=card_layout
)

card_market = widgets.VBox(
    [widgets.HTML("<b>Filtres marche</b>"), w_price, w_trends],
    layout=card_layout
)

bottom_row = widgets.HBox(
    [card_primary, score_card, card_geo, card_market],
    layout=widgets.Layout(width="100%", align_items="stretch", gap="12px", flex_wrap="wrap")
)

ui_final = widgets.VBox(
    [top_row, bottom_row],
    layout=widgets.Layout(width="100%", gap="12px")
)

clear_output(wait=True)
display(ui_final)

# Events
w_reset_deps.on_click(_on_reset)
for cb in dep_checkboxes.values():
    cb.observe(_on_deps_change, names="value")
for w in [w_type, w_score, w_mode, w_commune, w_zone_saturee, w_price, w_trends]:
    w.observe(_render, names="value")

_render()