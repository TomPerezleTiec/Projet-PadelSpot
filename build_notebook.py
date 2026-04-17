"""
build_notebook.py – Construit padelspot.ipynb proprement.
"""
import json
from pathlib import Path

BASE = Path(__file__).parent

# ── Lecture sources ───────────────────────────────────────────────────────────
with open(BASE / "padelspot2.ipynb", encoding="utf-8-sig") as f:
    old_nb = json.load(f)

with open(BASE / "padelspot copy.ipynb", encoding="utf-8-sig") as f:
    copy_nb = json.load(f)

with open(BASE / "padelspot_clean.py", encoding="utf-8") as f:
    clean_lines = f.readlines()

old_cells = old_nb["cells"]
copy_cells = copy_nb["cells"]


# ── Fix encodage latin-1/utf-8 dans les sources de l'ancien notebook ─────────
def fix_encoding(text: str) -> str:
    """Corrige les caracteres mal encodes (Ã© -> é, etc.)."""
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text


# ── Helpers notebook ─────────────────────────────────────────────────────────
def code_cell(source_lines: list[str], cell_id: str = "cell") -> dict:
    """Cree une cellule de code Jupyter depuis une liste de lignes."""
    lines = []
    for i, line in enumerate(source_lines):
        if i < len(source_lines) - 1:
            lines.append(line if line.endswith("\n") else line + "\n")
        else:
            lines.append(line.rstrip("\n"))
    return {
        "cell_type": "code",
        "execution_count": None,
        "id": cell_id,
        "metadata": {},
        "outputs": [],
        "source": lines,
    }


def md_cell(text: str, cell_id: str = "md") -> dict:
    """Cree une cellule markdown."""
    return {
        "cell_type": "markdown",
        "id": cell_id,
        "metadata": {},
        "source": [text],
    }


def clean_slice(start: int, end: int) -> list[str]:
    """Extrait les lignes [start..end] du fichier clean.py (1-indexed inclus)."""
    chunk = clean_lines[start - 1: end]
    result = []
    for i, line in enumerate(chunk):
        if i < len(chunk) - 1:
            result.append(line if line.endswith("\n") else line + "\n")
        else:
            result.append(line.rstrip("\n"))
    return result


def old_cell(cell_id: str, source_cells: list = old_cells) -> dict:
    """Recupere une cellule, corrige l'encodage, vide les outputs."""
    for c in source_cells:
        if c.get("id") == cell_id:
            cell = dict(c, execution_count=None, outputs=[])
            cell["source"] = [fix_encoding(line) for line in cell["source"]]
            return cell
    raise KeyError(f"Cellule introuvable : {cell_id}")


# ── Construction des cellules ─────────────────────────────────────────────────
cells = []

# ═══════════════════════════════════════════════════════════════
# INITIALISATION
# ═══════════════════════════════════════════════════════════════
cells.append(md_cell(
    "# 🎾 PadelSpot – Pipeline de Données\n\n"
    "Ce notebook orchestre le traitement complet des données pour PadelSpot.\n\n"
    "**Sources de données consolidées :**\n"
    "1. Demandes foncières (DVF) – *Prix de l'immobilier*\n"
    "2. Revenus (Filosofi) – *Niveau de vie moyen par carreau de 200m*\n"
    "3. Concurrence et cibles (Equipements sportifs) – *Densité de l'offre existante*\n"
    "4. Topologie (OpenStreetMap PBF) – *Accès aux transports et réseau routier*\n"
    "5. Intérêt (Google Trends) – *Recherche Web 'padel'*",
    "md_intro"))

cells.append(md_cell("### Installation des dépendances", "md_install"))
cells.append(code_cell(["%pip install pyspark plotly ipywidgets pandas numpy anywidget osmium pyproj xgboost -q"], "install_deps"))

cells.append(md_cell(
    "## 0 – Initialisation Spark\n\n"
    "On démarre une session Spark locale avec 4 Go de mémoire driver. "
    "On utilise `getOrCreate()` pour éviter de relancer une session si elle existe déjà — "
    "pratique quand on réexécute des cellules isolées.",
    "md_init"))

cells.append(code_cell(clean_slice(1, 44), "imports_get_spark"))

cells.append(code_cell([
    "spark = get_spark('PadelSpot_Pipeline', driver_memory='4g')",
    "spark",
], "call_get_spark"))

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 – DVF
# ═══════════════════════════════════════════════════════════════
cells.append(md_cell(
    "---\n## 1 – Données DVF (Demandes de Valeurs Foncières)\n\n"
    "Le DVF recense toutes les transactions immobilières en France. "
    "L'idée ici est simple : estimer le **coût au m²** d'un local commercial "
    "par commune, pour identifier les zones économiquement accessibles à un porteur de projet.\n\n"
    "On charge 5 à 6 fichiers annuels (2020-2025), on filtre sur les *locaux industriels et commerciaux* "
    "de plus de 200m², et on calcule la médiane du prix/m² par commune.",
    "md_dvf"))

cells.append(md_cell("### 1.1 – Chargement brut", "md_dvf_load"))
cells.append(code_cell(clean_slice(47, 128), "fn_load_dvf_raw"))

cells.append(md_cell(
    "Le schéma est construit dynamiquement depuis l'en-tête du premier fichier "
    "pour éviter un `inferSchema` coûteux sur 20M de lignes.",
    "md_dvf_load_call"))
cells.append(code_cell([
    "df_dvf_raw, has_gps = load_dvf_raw(spark)",
    "df_dvf_raw.printSchema()",
], "call_load_dvf_raw"))

cells.append(md_cell("### 1.2 – Nettoyage métier", "md_dvf_clean"))
cells.append(code_cell(clean_slice(131, 185), "fn_clean_dvf"))

cells.append(md_cell(
    "Après nettoyage, on s'attend à garder ~150k-200k transactions pertinentes sur les ~20M brutes.",
    "md_dvf_clean_call"))
cells.append(code_cell([
    "df_dvf_clean = clean_dvf(df_dvf_raw, has_gps)",
    "print(f'Transactions DVF nettoyees : {df_dvf_clean.count()}')",
    "df_dvf_clean.select(",
    "    'code_departement', 'code_commune', 'valeur_fonciere', 'surface_reelle_bati', 'prix_m2'",
    ").show(10, truncate=False)",
], "call_clean_dvf"))

cells.append(md_cell("### 1.3 – Agrégation par commune", "md_dvf_agg"))
cells.append(code_cell(clean_slice(188, 222), "fn_aggregate_dvf"))

cells.append(md_cell(
    "On utilise `percentile_approx` plutôt que `avg` pour être robuste aux valeurs aberrantes "
    "(ventes atypiques à Paris ou dans des ZAE industrielles). "
    "Le résultat est sauvegardé en Parquet partitionné par département.",
    "md_dvf_agg_call"))
cells.append(code_cell([
    "OUTPUT_DVF = '/home/jovyan/work/data/output/dvf_clean/'",
    "df_dvf_commune = aggregate_dvf_by_commune(df_dvf_clean, OUTPUT_DVF)",
    "df_dvf_commune.show(10, truncate=False)",
], "call_aggregate_dvf"))

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2 – FILOSOFI
# ═══════════════════════════════════════════════════════════════
cells.append(md_cell(
    "---\n## 2 – Données Filosofi (INSEE 2021)\n\n"
    "Filosofi fournit des statistiques sur des *carreaux* de 200m × 200m couvrant toute la France. "
    "C'est la source la plus fine qu'on ait pour croiser revenus, population et démographie "
    "sans passer par les communes (trop agrégées).\n\n"
    "Les colonnes clés qu'on utilise :\n"
    "- `Ind` – population estimée dans le carreau\n"
    "- `Ind_snv` – revenu médian par unité de consommation\n"
    "- `Ind_18_24` / `Ind_25_39` – tranches d'âge cible pour le padel\n"
    "- `X_c` / `Y_c` – coordonnées en projection EPSG:3035 (à convertir en WGS84)\n\n"
    "Un point de vigilance : les fichiers changent de noms de colonnes entre millésimes, "
    "d'où le mapping de variantes dans le code.",
    "md_filo"))

cells.append(md_cell("### 2.1 – Constantes et mapping de colonnes", "md_filo_consts"))
cells.append(code_cell(clean_slice(225, 261), "filo_constants"))

cells.append(md_cell("### 2.2 – Chargement brut", "md_filo_load"))
cells.append(code_cell(clean_slice(264, 346), "fn_load_filosofi_raw"))

cells.append(md_cell(
    "Filosofi 2021 est découpé en 3 fichiers CSV (métropole, Martinique, Réunion). "
    "On charge uniquement la métropole ici — environ 2,3M de carreaux bruts.",
    "md_filo_load_call"))
cells.append(code_cell([
    "df_filosofi_raw = load_filosofi_raw(spark)",
    "print(f'Lignes brutes Filosofi : {df_filosofi_raw.count()}')",
    "df_filosofi_raw.printSchema()",
], "call_load_filosofi_raw"))

cells.append(md_cell("### 2.3 – Cast numérique et filtrage", "md_filo_cast"))
cells.append(code_cell(clean_slice(349, 394), "fn_cast_filter_filosofi"))

cells.append(md_cell(
    "On filtre sur `I_est_cr = 1` (carreaux statistiquement fiables) "
    "et on élimine les carreaux sans population ni revenu. Il en reste ~1,8M.",
    "md_filo_cast_call"))
cells.append(code_cell([
    "df_filosofi_typed = cast_filosofi_numeric(df_filosofi_raw)",
    "df_filosofi_clean = filter_filosofi(df_filosofi_typed)",
    "print(f'Lignes apres nettoyage : {df_filosofi_clean.count()}')",
    "df_filosofi_clean.select('IdINSPIRE', 'code_commune_insee', 'I_est_cr', 'Ind', 'Ind_snv', 'X_c', 'Y_c').show(10, truncate=False)",
], "call_cast_filter_filosofi"))

cells.append(md_cell(
    "### 2.4 – Conversion de projection : EPSG:3035 → WGS84\n\n"
    "Les coordonnées Filosofi sont en *Lambert Equal Area* (EPSG:3035), pas en GPS standard. "
    "On utilise une `pandas_udf` vectorisée avec `pyproj` pour la conversion — "
    "beaucoup plus rapide qu'une UDF Python classique ligne par ligne.",
    "md_filo_geo"))
cells.append(code_cell(clean_slice(397, 441), "fn_convert_epsg"))

cells.append(md_cell(
    "Après conversion, on filtre les carreaux hors France métropolitaine "
    "(lat ∈ [41, 51.5], lon ∈ [-5.5, 10.5]).",
    "md_filo_geo_call"))
cells.append(code_cell([
    "df_filosofi_geo = convert_epsg3035_to_wgs84(df_filosofi_clean)",
    "print(f'Lignes apres conversion GPS + filtre geo : {df_filosofi_geo.count()}')",
    "df_filosofi_geo.select('IdINSPIRE', 'Longitude', 'Latitude').show(10, truncate=False)",
], "call_convert_epsg"))

cells.append(md_cell(
    "### 2.5 – Feature engineering\n\n"
    "On calcule deux indicateurs dérivés :\n"
    "- `part_cible_padel` = (18-24 ans + 25-39 ans) / population totale du carreau\n"
    "- `score_revenu` = normalisation min-max du revenu médian dans [0, 1]\n\n"
    "On extrait aussi le `code_departement` depuis le code commune INSEE "
    "(avec fallback sur l'identifiant du carreau).",
    "md_filo_feat"))
cells.append(code_cell(clean_slice(444, 510), "fn_engineer_filosofi"))

cells.append(md_cell(
    "Sauvegarde en Parquet partitionné par département — "
    "ça accélère les jointures des étapes suivantes.",
    "md_filo_feat_call"))
cells.append(code_cell([
    "OUTPUT_FILOSOFI = '/home/jovyan/work/data/output/filosofi_clean/'",
    "df_filosofi_final = engineer_filosofi_features(df_filosofi_geo)",
    "(",
    "    df_filosofi_final.write",
    "    .mode('overwrite')",
    "    .partitionBy('code_departement')",
    "    .parquet(OUTPUT_FILOSOFI)",
    ")",
    "print(f'Filosofi sauvegarde dans : {OUTPUT_FILOSOFI}')",
    "print(f'Carreaux finaux : {df_filosofi_final.count()}')",
    "df_filosofi_final.select(",
    "    'IdINSPIRE', 'code_commune_insee', 'code_departement',",
    "    'Ind', 'Ind_snv', 'part_cible_padel', 'score_revenu', 'Longitude', 'Latitude'",
    ").show(10, truncate=False)",
], "call_engineer_filosofi"))

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 3 – CONCURRENCE
# ═══════════════════════════════════════════════════════════════
cells.append(md_cell(
    "---\n## 3 – Clubs existants (RES + SIRENE)\n\n"
    "Pour évaluer la concurrence autour d'un carreau, on a besoin de savoir où sont "
    "déjà implantés les clubs de padel. On croise deux sources :\n\n"
    "- **RES (Recensement des Équipements Sportifs)** – base officielle du Ministère des Sports\n"
    "- **SIRENE** – base des entreprises, filtrée sur les codes APE liés au padel/tennis\n\n"
    "Le RES est plus fiable sur les équipements publics, SIRENE capte mieux les clubs privés. "
    "On les fusionne et on déduplique par géolocalisation.",
    "md_conc"))

cells.append(md_cell("### 3.1 – Source RES", "md_conc_res"))
cells.append(code_cell(clean_slice(518, 580), "fn_load_res_padel"))

cells.append(md_cell(
    "Le RES nécessite une jointure entre deux fichiers (équipements et installations) "
    "pour reconstituer les coordonnées GPS de chaque club.",
    "md_conc_res_call"))
cells.append(code_cell([
    "df_res_std = load_res_padel(",
    "    spark,",
    "    equip_path='/home/jovyan/work/data/data-es-equipement.csv',",
    "    install_path='/home/jovyan/work/data/data-es-installation.csv',",
    ")",
    "print(f'Clubs RES Padel : {df_res_std.count()}')",
    "df_res_std.show(5, truncate=False)",
], "call_load_res_padel"))

cells.append(md_cell("### 3.2 – Source SIRENE", "md_conc_sirene"))
cells.append(code_cell(clean_slice(583, 674), "fn_load_sirene_padel"))

cells.append(md_cell(
    "SIRENE contient ~30M d'établissements. On filtre d'abord sur les codes APE "
    "pertinents avant la jointure avec les unités légales — sinon ça prend une éternité.",
    "md_conc_sirene_call"))
cells.append(code_cell([
    "df_sirene_std = load_sirene_padel(",
    "    spark,",
    "    etab_path='/home/jovyan/work/data/StockEtablissement_utf8.parquet',",
    "    ul_path='/home/jovyan/work/data/StockUniteLegale_utf8.parquet',",
    ")",
    "print(f'Clubs SIRENE Padel : {df_sirene_std.count()}')",
    "df_sirene_std.show(5, truncate=False)",
], "call_load_sirene_padel"))

cells.append(md_cell(
    "### 3.3 – Déduplication\n\n"
    "Un même club peut apparaître dans les deux sources. "
    "On déduplique en regroupant les clubs à moins de 100m l'un de l'autre (distance Haversine). "
    "La source RES est prioritaire en cas de doublon.",
    "md_conc_dedup"))
cells.append(code_cell(clean_slice(677, 730), "fn_deduplicate_clubs"))

cells.append(md_cell(
    "La répartition par source après déduplication donne une idée de la complémentarité "
    "des deux bases.",
    "md_conc_dedup_call"))
cells.append(code_cell([
    "OUTPUT_CONCURRENCE = '/home/jovyan/work/data/output/concurrence_padel/'",
    "df_clubs_dedup = deduplicate_padel_clubs(df_res_std, df_sirene_std, OUTPUT_CONCURRENCE)",
    "df_clubs_dedup.groupBy('Source').count().show()",
    "df_clubs_dedup.show(10, truncate=False)",
], "call_deduplicate_clubs"))

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 4 – OSM (cellules originales)
# ═══════════════════════════════════════════════════════════════
cells.append(md_cell(
    "---\n## 4 – Accessibilité (OpenStreetMap)\n\n"
    "Un club de padel bien situé doit être facilement accessible. "
    "On extrait depuis OpenStreetMap deux types de données :\n\n"
    "- **Arrêts de transport en commun** (bus, tram, métro, gares) — "
    "pour estimer la densité TC autour de chaque carreau\n"
    "- **Axes routiers principaux** (autoroute, nationale, voie rapide) — "
    "pour mesurer la proximité aux grands axes\n\n"
    "Le fichier PBF OpenStreetMap de France fait ~4 Go, donc le parsing "
    "est délégué à un sous-processus `pyosmium` pour ne pas bloquer le kernel Jupyter.",
    "md_osm"))

cells.append(md_cell(
    "### 4.1 – Parsing OSM\n\n"
    "Le parser tourne en deux passes : d'abord les noeuds (arrêts TC), "
    "puis les ways (routes). Les résultats sont écrits dans deux CSV intermédiaires.",
    "md_osm_1"))
cells.append(old_cell("f0e8be6b"))

cells.append(md_cell("### 4.2 – Ingestion Spark et schéma explicite", "md_osm_2"))
cells.append(old_cell("407a141e"))

cells.append(md_cell(
    "### 4.3 – Indicateurs d'accessibilité par carreau\n\n"
    "Pour chaque carreau Filosofi, on calcule :\n"
    "- `densite_tc` : nombre d'arrêts TC dans un rayon de 1 km\n"
    "- `proximite_axe` : distance au tronçon routier principal le plus proche",
    "md_osm_3"))
cells.append(old_cell("b271363f"))

cells.append(md_cell(
    "### 4.4 – Sauvegarde\n\n"
    "Les scores d'accessibilité sont sauvegardés en Parquet "
    "pour être joints au score composite à l'étape 6.",
    "md_osm_4"))
cells.append(old_cell("8ae65d47"))

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 5 – GOOGLE TRENDS
# ═══════════════════════════════════════════════════════════════
cells.append(md_cell(
    "---\n## 5 – Demande latente (Google Trends)\n\n"
    "Google Trends donne un indice de popularité des recherches pour le terme *padel* "
    "par région française. C'est un proxy de la demande latente non satisfaite.\n\n"
    "La donnée est à la maille **région** (13 régions + Corse), "
    "donc on doit la projeter sur les départements via un mapping hardcodé. "
    "Un indice élevé dans une région peu équipée = opportunité forte.",
    "md_trends"))

cells.append(md_cell(
    "### 5.1 – Chargement et normalisation\n\n"
    "Le fichier `geoMap.csv` est exporté depuis Google Trends. "
    "On normalise les régions (accents, tirets) et on impute les valeurs manquantes "
    "par la médiane nationale.",
    "md_trends_load"))
cells.append(code_cell(clean_slice(733, 850), "fn_load_trends"))

cells.append(md_cell(
    "On s'attend à 13 lignes (une par région métropolitaine). "
    "La médiane nationale est affichée — elle servira d'imputation pour les départements non couverts.",
    "md_trends_load_call"))
cells.append(code_cell([
    "import os",
    "",
    "TRENDS_CANDIDATES = [",
    "    '/home/jovyan/work/data/trends/geoMap.csv',",
    "    '/home/jovyan/work/data/geoMap.csv',",
    "]",
    "trends_path = next((p for p in TRENDS_CANDIDATES if os.path.exists(p)), None)",
    "if trends_path is None:",
    "    raise FileNotFoundError('geoMap.csv introuvable.')",
    "",
    "df_trends_clean, median_indice = load_trends(spark, trends_path)",
    "print(f'Lignes Trends lues : {df_trends_clean.count()}')",
    "df_trends_clean.show(30, truncate=False)",
], "call_load_trends"))

cells.append(md_cell(
    "### 5.2 – Projection région → départements\n\n"
    "On explode le mapping région → liste de départements pour obtenir "
    "une ligne par département. Une validation vérifie que les 96 départements "
    "métropolitains sont bien couverts.",
    "md_trends_explode"))
cells.append(code_cell(clean_slice(853, 901), "fn_explode_trends"))

cells.append(md_cell(
    "Si tout est bon : 96 lignes, une par département.",
    "md_trends_explode_call"))
cells.append(code_cell([
    "df_trends_by_departement = explode_trends_to_departements(spark, df_trends_clean)",
    "df_trends_by_departement.orderBy('code_departement').show(100, truncate=False)",
], "call_explode_trends"))

cells.append(md_cell(
    "### 5.3 – Jointure avec les carreaux Filosofi\n\n"
    "On fait un **broadcast join** : le DataFrame Trends (~100 lignes) "
    "est diffusé sur tous les workers pour éviter un shuffle sur les 1,8M de carreaux.",
    "md_trends_join"))
cells.append(code_cell([
    "import pyspark.sql.functions as F",
    "",
    "OUTPUT_TRENDS = '/home/jovyan/work/data/output/trends_joined/'",
    "df_filo_source = spark.read.parquet(OUTPUT_FILOSOFI)",
    "df_trends_small = df_trends_by_departement.select('code_departement', 'indice_trends')",
    "",
    "df_filosofi_trends = (",
    "    df_filo_source",
    "    .join(F.broadcast(df_trends_small), on='code_departement', how='left')",
    "    .withColumnRenamed('indice_trends', 'indice_demande_trends')",
    "    .withColumn(",
    "        'indice_demande_trends',",
    "        F.coalesce(F.col('indice_demande_trends'), F.lit(int(median_indice))),",
    "    )",
    ")",
    "(",
    "    df_filosofi_trends.write",
    "    .mode('overwrite')",
    "    .partitionBy('code_departement')",
    "    .parquet(OUTPUT_TRENDS)",
    ")",
    "print(f'Trends joint sauvegarde dans : {OUTPUT_TRENDS}')",
    "print(f'Carreaux enrichis : {df_filosofi_trends.count()}')",
], "call_trends_join"))

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 6 – SCORE COMPOSITE
# ═══════════════════════════════════════════════════════════════
cells.append(md_cell(
    "---\n## 6 – Score composite d'implantation\n\n"
    "C'est l'étape centrale du projet : on agrège toutes les sources en un seul score "
    "par carreau de 200m. Le score est une moyenne pondérée de 5 dimensions :\n\n"
    "| Dimension | Poids (scénario base) |\n"
    "|---|---|\n"
    "| Concurrence (nb clubs proches) | 30% |\n"
    "| Accessibilité (TC + axes) | 25% |\n"
    "| Revenu des ménages | 20% |\n"
    "| Prix immobilier | 15% |\n"
    "| Demande Trends | 10% |\n\n"
    "Plusieurs jeux de poids (`WEIGHT_SETS`) sont testés pour mesurer "
    "la sensibilité du classement aux hypothèses.",
    "md_score"))

cells.append(md_cell(
    "### 6.1 – Normalisation et poids\n\n"
    "Chaque dimension est ramenée dans [0, 1] via min-max avant agrégation. "
    "Le score concurrence est *inversé* : plus il y a de clubs proches, plus le score est bas.",
    "md_score_utils"))
cells.append(code_cell(clean_slice(904, 945), "fn_score_utils"))

cells.append(md_cell("### 6.2 – Chargement des sources", "md_score_init"))
cells.append(old_cell("6c41c572"))

cells.append(md_cell(
    "### 6.3 – Score concurrentiel\n\n"
    "Pour chaque carreau, on compte les clubs dans un rayon de 5 km "
    "via la distance Haversine. On utilise une Pandas UDF pour rester vectorisé.",
    "md_score_conc"))
cells.append(old_cell("6ca26d2e"))

cells.append(md_cell(
    "### 6.4 – Assemblage du score final\n\n"
    "On joint tous les scores partiels et on calcule le score composite "
    "pour chaque jeu de poids. Le score `base` est retenu comme référence.",
    "md_score_final"))
cells.append(old_cell("322e06a8"))

cells.append(md_cell(
    "### 6.5 – Export des meilleures zones\n\n"
    "On isole le top des carreaux selon le score composite. "
    "L'export GeoJSON permet de visualiser les zones dans QGIS ou Folium.",
    "md_score_top"))
cells.append(old_cell("35d15aa2"))

cells.append(md_cell(
    "### 6.6 – Export complet France",
    "md_score_export"))
cells.append(old_cell("63800513"))

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 7 – DASH READY
# ═══════════════════════════════════════════════════════════════
cells.append(md_cell(
    "---\n## 7 – Préparation des exports (Dash Ready)\n\n"
    "Avant de visualiser, on prépare des tables allégées et normalisées "
    "adaptées à la consommation par une application (Dash, Streamlit, Plotly).\n\n"
    "Plusieurs tables sont générées :\n"
    "- `dash_carreaux_full` – tous les carreaux scorés (Parquet)\n"
    "- `dash_communes_agg` – agrégation par commune\n"
    "- `dash_clubs` – liste des clubs avec leur zone d'implantation\n"
    "- `dash_departements_stats` – statistiques par département\n"
    "- `dash_top_zones` – top zones (Parquet + GeoJSON)",
    "md_dash"))

cells.append(md_cell(
    "### 7.1 – Schéma de sortie : `apply_dash_schema()`\n\n"
    "Cette fonction standardise les types et remplace les nulls "
    "par des valeurs par défaut cohérentes (0.0 pour les scores, -1 pour les entiers, 'N/A' pour les strings). "
    "On utilise un `fillna` groupé et un `select()` vectorisé pour éviter de fragmenter le DAG Spark.",
    "md_dash_helpers"))
cells.append(code_cell(clean_slice(948, 1071), "fn_dash_helpers"))

cells.append(md_cell(
    "### 7.2 – Construction de la table principale",
    "md_dash_1"))
cells.append(old_cell("0368dea1"))

cells.append(md_cell(
    "### 7.3 – Tables dérivées (communes, clubs, transport, départements)\n\n"
    "Le CSV `dash_clubs.csv` est celui utilisé par la carte interactive ci-dessous.",
    "md_dash_2"))
cells.append(old_cell("49bb2890"))

cells.append(md_cell("### 7.4 – Fichier de métadonnées JSON", "md_dash_3"))
cells.append(old_cell("42bf5b01"))

# ═══════════════════════════════════════════════════════════════
# VISUALISATION
# ═══════════════════════════════════════════════════════════════
cells.append(md_cell(
    "---\n## Visualisation – Carte interactive des clubs Padel\n\n"
    "Carte Plotly/ipywidgets affichant tous les clubs recensés sur une carte OSM. "
    "Les filtres permettent d'explorer par département, type de club, score d'implantation, "
    "commune et saturation de zone. La taille des markers reflète le nombre de courts.",
    "md_viz"))

cells.append(md_cell("### Logique de création de la carte Plotly", "md_viz_1"))
cells.append(old_cell("7fb5ad27", source_cells=copy_cells))

cells.append(md_cell("### Widgets interactifs et Layout UI", "md_viz_2"))
cells.append(old_cell("15ad24db", source_cells=copy_cells))

# ═══════════════════════════════════════════════════════════════
# ÉTAPES 8 ET 9
# ═══════════════════════════════════════════════════════════════
cells.append(md_cell(
    "---\n## 8 – Export Plotly (bundle ZIP)\n\n"
    "On génère un bundle ZIP contenant les CSV et GeoJSON nécessaires "
    "pour alimenter une application Plotly Dash externe.",
    "md_etape8"))
cells.append(old_cell("66f4a934"))

cells.append(md_cell(
    "---\n## 9 – Correctif carte Plotly\n\n"
    "Patch appliqué pour corriger l'affichage de la carte fond de plan "
    "dans certaines versions de Plotly.",
    "md_etape9"))
cells.append(old_cell("bed13d72"))

# ── Assemblage ────────────────────────────────────────────────────────────────
new_nb = {
    "nbformat": 4,
    "nbformat_minor": 5,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.11.0"},
    },
    "cells": cells,
}

out_path = BASE / "padelspot.ipynb"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(new_nb, f, ensure_ascii=False, indent=1)

print(f"Notebook genere : {out_path}")
print(f"Nombre de cellules : {len(cells)}")
