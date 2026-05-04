"""
Generated from padelspot.ipynb.

This file is intentionally generated from the notebook so the pipeline
can be versioned and reviewed as plain Python.
"""

from __future__ import annotations
# ===== Cell 001 | markdown =====
# # 🎾 PadelSpot – Pipeline de Données
#
# Ce notebook explique, étape par étape, comment des données brutes sont transformées en information claire pour décider où implanter un club de padel.
#
# Le fil conducteur est simple. Chaque bloc prend une source de données, la nettoie, la rend comparable avec les autres, puis ajoute une pièce au score final.
#
# Exemple réel observé dans les sorties: 1 809 025 carreaux ont été conservés après préparation géographique.

# ===== Cell 002 | markdown =====
# ### Installation des dépendances
#
# Cette cellule installe les outils nécessaires pour exécuter le projet. Sans cette étape, les cellules suivantes peuvent échouer même si le code est correct.

# ===== Cell 003 | markdown =====
# Dans cette première partie, il n’y a pas encore de transformation métier. L’objectif est de préparer un environnement stable avec les bonnes bibliothèques. Quand cette cellule fonctionne, la suite du notebook est beaucoup plus fiable.

# ===== Cell 004 | code =====
# NOTEBOOK_MAGIC: %pip install pyspark plotly ipywidgets pandas numpy anywidget osmium pyproj xgboost -q

# ===== Cell 005 | markdown =====
# ## 0 – Initialisation Spark
#
# Spark est le moteur de calcul utilisé dans tout le notebook. Cette étape prépare ce moteur pour traiter des volumes importants, faire les jointures et produire des agrégations stables.

# ===== Cell 006 | markdown =====
# Ici, Spark devient le moteur principal du notebook. Les grosses jointures et les agrégations seront exécutées avec ce moteur. Cette étape sert surtout à garantir des calculs stables sur de gros volumes de données.

# ===== Cell 007 | code =====
"""
PadelSpot – Pipeline PySpark optimisé production.

Directives appliquées :
- Aucun emoji dans les logs.
- Boucles Python for/withColumn remplacées par select() vectorisé ou fillna() groupé.
- UDF Python normalize_region remplacée par regexp_replace natif (22 régions fixes).
- Lecture des en-têtes CSV via Python (pas d'action Spark pour 1 ligne).
- Docstrings format Google sur chaque fonction utilitaire.
- PEP 8 strict, noms de variables explicites.
- SparkSession unique via getOrCreate() ; guard 'if "spark" not in globals()' dans chaque section.
"""

# ============================================================
# INIT COMMUNE – Session Spark réutilisée dans tout le notebook
# ============================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, IntegerType, FloatType, ShortType, ArrayType,
)


def get_spark(app_name: str = "PadelSpot", driver_memory: str = "4g") -> SparkSession:
    """Retourne une SparkSession locale optimisée, ou la crée si absente.

    Args:
        app_name: Nom affiché dans la Spark UI.
        driver_memory: Mémoire allouée au driver (ex. '4g', '6g').

    Returns:
        SparkSession configurée avec Arrow et log level WARN.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ===== Cell 008 | code =====
spark = get_spark('PadelSpot_Pipeline', driver_memory='4g')
spark

# ===== Cell 009 | markdown =====
# ---
# ## 1 – Données DVF (Demandes de Valeurs Foncières)
#
# Dans cette étape, on passe de transactions immobilières brutes à un indicateur utile par commune: le prix médian au mètre carré.
#
# Exemple réel observé: 15 602 communes agrégées en sortie.

# ===== Cell 010 | markdown =====
# Cette étape démarre la transformation métier avec les données DVF. On passe de transactions immobilières brutes à un indicateur simple par zone. Dans les sorties, il faut surtout observer le nombre de lignes après nettoyage et le prix au mètre carré, car cette information est utilisée plus loin dans le score.

# ===== Cell 011 | markdown =====
# ### 1.1 – Chargement brut
#
# Dans cette sous-étape, les fichiers DVF sont lus sans appliquer encore de logique métier complexe. Le but est de récupérer une base complète et structurée pour pouvoir ensuite filtrer proprement.
#
# La bonne lecture de la sortie consiste à vérifier que les colonnes attendues existent bien, puis à vérifier le volume initial. On accepte un volume élevé à ce stade, car le nettoyage vient ensuite.
#
# Illustration réelle observée dans les sorties: la table brute est ensuite ramenée à 15 602 communes après agrégation métier.

# ===== Cell 012 | code =====
# ============================================================
# ÉTAPE 1 – Ingestion et nettoyage DVF
# Préparation des transactions immobilières pour estimer le prix
# médian au m² des locaux industriels/commerciaux par commune.
# ============================================================

import glob

# Colonnes utiles pour l'étape 1
DVF_REQUIRED_COLS = [
    "Date mutation",
    "Valeur fonciere",
    "Code postal",
    "Code departement",
    "Code commune",
    "Type local",
    "Surface reelle bati",
    "Longitude",
    "Latitude",
]

# Patterns de recherche des fichiers DVF
DVF_PATTERNS = [
    "/home/jovyan/work/data/dvf/*.txt",
    "/home/jovyan/work/data/valeursfoncieres-*/*.txt",
]


def load_dvf_raw(spark: SparkSession) -> "DataFrame":
    """Charge les fichiers DVF bruts avec schéma explicite.

    Construit le schéma depuis l'en-tête du premier fichier (lecture
    Python, pas d'action Spark). Projette uniquement les 9 colonnes
    métier utiles. Ajoute Longitude/Latitude nulles si absentes.

    Args:
        spark: Session Spark active.

    Returns:
        DataFrame brut avec les 9 colonnes DVF requises.

    Raises:
        FileNotFoundError: Si aucun fichier DVF n'est trouvé.
    """
    dvf_paths = sorted({p for pat in DVF_PATTERNS for p in glob.glob(pat)})
    if not dvf_paths:
        raise FileNotFoundError(
            "Aucun fichier DVF trouve dans dvf/ ou valeursfoncieres-*/"
        )

    print(f"Fichiers DVF detectes : {len(dvf_paths)}")
    for path in dvf_paths:
        print(f"  - {path}")

    # Lecture de l'en-tête via Python (évite un job Spark pour 1 ligne)
    with open(dvf_paths[0], encoding="utf-8") as f_hdr:
        header_line = f_hdr.readline().rstrip("\n")
    all_cols = header_line.split("|")

    full_schema = StructType(
        [StructField(col, StringType(), True) for col in all_cols]
    )

    df_dvf_full = (
        spark.read
        .option("header", True)
        .option("sep", "|")
        .schema(full_schema)
        .csv(dvf_paths)
    )

    # Certains millésimes DVF ne contiennent pas les coordonnées GPS
    has_gps = ("Longitude" in df_dvf_full.columns) and ("Latitude" in df_dvf_full.columns)
    if not has_gps:
        print("Colonnes GPS absentes dans les sources DVF : ajout a null.")
        df_dvf_full = (
            df_dvf_full
            .withColumn("Longitude", F.lit(None).cast("string"))
            .withColumn("Latitude", F.lit(None).cast("string"))
        )

    return df_dvf_full.select(*DVF_REQUIRED_COLS), has_gps

# ===== Cell 013 | markdown =====
# Le schéma est construit dynamiquement depuis l'en-tête du premier fichier pour éviter un `inferSchema` coûteux sur 20M de lignes.

# ===== Cell 014 | code =====
df_dvf_raw, has_gps = load_dvf_raw(spark)
df_dvf_raw.printSchema()

# ===== Cell 015 | markdown =====
# ### 1.2 – Nettoyage métier
#
# Cette sous-étape retire les lignes qui ne correspondent pas au besoin d'analyse. On cible les transactions réellement utiles à l'étude d'implantation.
#
# L'idée est simple. Mieux vaut moins de lignes mais des lignes cohérentes, plutôt qu'un gros volume bruité qui dégrade la qualité du score.
#
# Dans les sorties DataFrame, il faut observer le nombre de lignes conservées et la cohérence des colonnes de prix et surface.

# ===== Cell 016 | code =====
def clean_dvf(df_dvf_raw: "DataFrame", has_gps_columns: bool) -> "DataFrame":
    """Nettoie et type les transactions DVF.

    - Normalise les décimales françaises (virgule -> point).
    - Caste en types numériques.
    - Applique les filtres métier (type local, surface > 200 m², valeur > 0).
    - Filtre géographique France métropolitaine si GPS disponible.
    - Calcule le prix au m².

    Args:
        df_dvf_raw: DataFrame brut avec les 9 colonnes DVF.
        has_gps_columns: True si Longitude/Latitude sont réellement présentes.

    Returns:
        DataFrame propre avec prix_m2 calculé.
    """
    # Normalisation décimale + cast en un seul select (1 plan logique)
    df_dvf_clean = df_dvf_raw.select(
        F.col("Date mutation").alias("date_mutation"),
        F.col("Code postal").alias("code_postal"),
        F.col("Code departement").alias("code_departement"),
        F.col("Code commune").alias("code_commune"),
        F.col("Type local").alias("type_local"),
        F.regexp_replace(F.col("Valeur fonciere"), ",", ".").cast("double").alias("valeur_fonciere"),
        F.regexp_replace(F.col("Surface reelle bati"), ",", ".").cast("double").alias("surface_reelle_bati"),
        F.regexp_replace(F.col("Longitude"), ",", ".").cast("double").alias("longitude"),
        F.regexp_replace(F.col("Latitude"), ",", ".").cast("double").alias("latitude"),
    )

    # Filtre métier de base (indépendant des coordonnées)
    base_filter = (
        (F.col("type_local") == "Local industriel. commercial ou assimilé")
        & F.col("valeur_fonciere").isNotNull()
        & F.col("surface_reelle_bati").isNotNull()
        & (F.col("valeur_fonciere") > 0)
        & (F.col("surface_reelle_bati") > 200)
    )

    # Filtre GPS uniquement si les colonnes existaient en source
    if has_gps_columns:
        gps_filter = (
            F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull()
            & F.col("latitude").between(41.0, 51.5)
            & F.col("longitude").between(-5.0, 10.0)
        )
    else:
        print("Filtre GPS ignore : colonnes Longitude/Latitude absentes des sources DVF.")
        gps_filter = F.lit(True)

    return (
        df_dvf_clean
        .filter(base_filter & gps_filter)
        .withColumn("prix_m2", F.col("valeur_fonciere") / F.col("surface_reelle_bati"))
    )

# ===== Cell 017 | markdown =====
# Après nettoyage, on s'attend à garder ~150k-200k transactions pertinentes sur les ~20M brutes.

# ===== Cell 018 | code =====
df_dvf_clean = clean_dvf(df_dvf_raw, has_gps)
print(f'Transactions DVF nettoyees : {df_dvf_clean.count()}')
df_dvf_clean.select(
    'code_departement', 'code_commune', 'valeur_fonciere', 'surface_reelle_bati', 'prix_m2'
).show(10, truncate=False)

# ===== Cell 019 | markdown =====
# ### 1.3 – Agrégation par commune
#
# Ici, les transactions nettoyées sont regroupées par commune pour produire un indicateur plus stable, le prix médian au mètre carré.
#
# Cette agrégation transforme une donnée transactionnelle en donnée territoriale. C'est cette forme qui peut être jointe correctement avec les autres sources du pipeline.
#
# Illustration réelle observée: 15 602 communes agrégées et une sortie structurée comme code_departement, code_commune, prix_median_m2, nb_transactions.

# ===== Cell 020 | code =====
def aggregate_dvf_by_commune(df_dvf_clean: "DataFrame", output_path: str) -> "DataFrame":
    """Agrège les transactions DVF par commune et sauvegarde en Parquet.

    Calcule le prix médian au m² via percentile_approx (robuste au volume)
    et le nombre de transactions commerciales. Sauvegarde partitionné par
    code_departement pour des lectures filtrées rapides.

    Args:
        df_dvf_clean: DataFrame DVF nettoyé avec prix_m2.
        output_path: Chemin de sortie Parquet.

    Returns:
        DataFrame agrégé par commune (code_departement, code_commune,
        prix_median_m2, nb_transactions).
    """
    df_dvf_commune = (
        df_dvf_clean
        .groupBy("code_departement", "code_commune")
        .agg(
            F.expr("percentile_approx(prix_m2, 0.5, 10000)").alias("prix_median_m2"),
            F.count("*").alias("nb_transactions"),
        )
        .orderBy(F.desc("nb_transactions"))
    )

    (
        df_dvf_commune.write
        .mode("overwrite")
        .partitionBy("code_departement")
        .parquet(output_path)
    )

    print(f"DVF agrege sauvegarde dans : {output_path}")
    print(f"Nombre de communes (lignes agregees) : {df_dvf_commune.count()}")
    return df_dvf_commune

# ===== Cell 021 | markdown =====
# On utilise `percentile_approx` plutôt que `avg` pour être robuste aux valeurs aberrantes (ventes atypiques à Paris ou dans des ZAE industrielles). Le résultat est sauvegardé en Parquet partitionné par département.

# ===== Cell 022 | code =====
OUTPUT_DVF = '/home/jovyan/work/data/output/dvf_clean/'
df_dvf_commune = aggregate_dvf_by_commune(df_dvf_clean, OUTPUT_DVF)
df_dvf_commune.show(10, truncate=False)

# ===== Cell 023 | markdown =====
# ---
# ## 2 – Données Filosofi (INSEE 2021)
#
# Cette section apporte la dimension socio-démographique fine, à l'échelle des carreaux de 200 mètres. On nettoie les colonnes, on convertit les coordonnées et on garde les zones exploitables.
#
# Exemple réel observé: 1 809 025 carreaux finaux.

# ===== Cell 024 | markdown =====
# Avec Filosofi, on passe à une maille géographique fine, le carreau de 200 mètres. C’est ici que l’analyse devient locale. Dans les tableaux, les colonnes de coordonnées, de population et de revenu montrent comment la donnée brute devient une donnée directement exploitable.

# ===== Cell 025 | markdown =====
# ### 2.1 – Constantes et mapping de colonnes
#
# Filosofi change parfois ses noms de colonnes selon les fichiers. Cette cellule définit donc une règle d'harmonisation pour éviter les erreurs de lecture dans les étapes suivantes.
#
# Ce travail paraît technique, mais il est essentiel. Sans harmonisation, deux fichiers contenant la même information peuvent être traités comme s'ils étaient différents.

# ===== Cell 026 | code =====
# ============================================================
# ÉTAPE 2 – Ingestion et nettoyage INSEE Filosofi 2021
# Préparation des carreaux 200m x 200m (démographie + revenus),
# conversion EPSG:3035 vers WGS84, feature engineering.
# ============================================================

# Colonnes canoniques Filosofi
FILOSOFI_CANONICAL_COLS = [
    "IdINSPIRE", "code_commune_insee", "I_est_cr",
    "Ind", "Ind_snv", "Men_pauv", "Ind_18_24", "Ind_25_39",
    "I_pauv", "X_c", "Y_c",
]

# Mapping variantes de noms entre millésimes
FILOSOFI_COL_CANDIDATES = {
    "IdINSPIRE": ["IdINSPIRE", "idcar_200m"],
    "code_commune_insee": ["code_commune_insee", "lcog_geo", "Code commune", "code_commune"],
    "I_est_cr": ["I_est_cr", "i_est_200"],
    "Ind": ["Ind", "ind"],
    "Ind_snv": ["Ind_snv", "ind_snv"],
    "Men_pauv": ["Men_pauv", "men_pauv"],
    "Ind_18_24": ["Ind_18_24", "ind_18_24"],
    "Ind_25_39": ["Ind_25_39", "ind_25_39"],
    "I_pauv": ["I_pauv", "i_pauv", "tx_pauv"],
    "X_c": ["X_c", "x_c"],
    "Y_c": ["Y_c", "y_c"],
}

FILOSOFI_NUMERIC_COLS = [
    "I_est_cr", "Ind", "Ind_snv", "Men_pauv",
    "Ind_18_24", "Ind_25_39", "I_pauv", "X_c", "Y_c",
]

FILOSOFI_PATTERNS = [
    "/home/jovyan/work/data/filosofi/Filosofi2021_carreaux_200m_csv/*.csv",
    "/home/jovyan/work/data/Filosofi2021_carreaux_200m_csv/*.csv",
]

# ===== Cell 027 | markdown =====
# ### 2.2 – Chargement brut
#
# Cette sous-étape lit les fichiers Filosofi avant filtrage fin. On constitue un socle complet pour ensuite appliquer les conversions et les contrôles qualité.
#
# Dans les sorties, il faut vérifier que les colonnes géographiques et démographiques sont présentes, car elles sont nécessaires au score final.

# ===== Cell 028 | code =====
def load_filosofi_raw(spark: SparkSession) -> "DataFrame":
    """Charge les CSV Filosofi avec schéma explicite et résolution des colonnes.

    Détecte le séparateur (';' ou ',') depuis l'en-tête Python. Résout les
    variantes de noms de colonnes entre millésimes. Extrait X_c/Y_c depuis
    IdINSPIRE si absentes.

    Args:
        spark: Session Spark active.

    Returns:
        DataFrame brut avec les colonnes canoniques Filosofi.

    Raises:
        FileNotFoundError: Si aucun CSV Filosofi n'est trouvé.
        ValueError: Si la colonne IdINSPIRE est introuvable.
    """
    filosofi_paths = sorted({p for pat in FILOSOFI_PATTERNS for p in glob.glob(pat)})
    if not filosofi_paths:
        raise FileNotFoundError(
            "Aucun CSV Filosofi trouve dans filosofi/Filosofi2021_carreaux_200m_csv/"
        )

    print(f"Fichiers Filosofi detectes : {len(filosofi_paths)}")
    for path in filosofi_paths[:10]:
        print(f"  - {path}")

    # Détection du séparateur via Python (pas d'action Spark)
    with open(filosofi_paths[0], encoding="utf-8") as f_hdr:
        header_line = f_hdr.readline().rstrip("\n")
    sep = ";" if ";" in header_line else ","
    all_cols = header_line.split(sep)

    print(f"Separateur detecte : '{sep}'")

    full_schema = StructType([StructField(c, StringType(), True) for c in all_cols])

    df_full = (
        spark.read
        .option("header", True)
        .option("sep", sep)
        .schema(full_schema)
        .csv(filosofi_paths)
    )

    # Résolution des variantes de noms de colonnes
    resolved = {
        canonical: next(
            (name for name in candidates if name in df_full.columns), None
        )
        for canonical, candidates in FILOSOFI_COL_CANDIDATES.items()
    }

    if resolved.get("IdINSPIRE") is None:
        raise ValueError(
            "Impossible d'identifier la colonne IdINSPIRE/idcar_200m dans les CSV Filosofi."
        )

    print(f"Mapping colonnes retenu : {resolved}")

    # Projection canonique – une seule passe select
    df_raw = df_full.select(
        *[
            F.col(resolved[c]).alias(c) if resolved.get(c) else F.lit(None).alias(c)
            for c in FILOSOFI_CANONICAL_COLS
        ]
    )

    # Fallback coordonnées depuis IdINSPIRE si X_c/Y_c absents
    if resolved.get("X_c") is None or resolved.get("Y_c") is None:
        df_raw = (
            df_raw
            .withColumn("Y_c", F.coalesce(
                F.col("Y_c"),
                F.regexp_extract(F.col("IdINSPIRE"), "N([0-9]+)E", 1)
            ))
            .withColumn("X_c", F.coalesce(
                F.col("X_c"),
                F.regexp_extract(F.col("IdINSPIRE"), "E([0-9]+)", 1)
            ))
        )

    return df_raw

# ===== Cell 029 | markdown =====
# ### 2.3 – Cast numérique et filtrage
#
# Ici, les colonnes sont converties dans les bons types numériques, puis les lignes non exploitables sont retirées. Cette étape améliore la fiabilité statistique de la base.
#
# Illustration réelle observée: 1 809 025 carreaux sont conservés après préparation, ce qui montre un volume important mais propre pour l'analyse.

# ===== Cell 030 | code =====
df_filosofi_raw = load_filosofi_raw(spark)
print(f'Lignes brutes Filosofi : {df_filosofi_raw.count()}')
df_filosofi_raw.printSchema()

# ===== Cell 031 | markdown =====
# ### 2.4 – Conversion de projection : EPSG:3035 → WGS84
#
# La donnée source n'est pas dans le format de coordonnées utilisé par les cartes web. Cette cellule convertit donc les coordonnées vers un format standard latitude/longitude.
#
# Cette conversion est indispensable pour que les points apparaissent au bon endroit sur la carte finale.

# ===== Cell 032 | code =====
def cast_filosofi_numeric(df_filosofi_raw: "DataFrame") -> "DataFrame":
    """Normalise et caste les colonnes numériques Filosofi en une seule passe.

    Remplace les virgules décimales françaises par des points, puis caste
    en DoubleType via try_cast (robuste aux valeurs manquantes).

    Args:
        df_filosofi_raw: DataFrame Filosofi avec colonnes StringType.

    Returns:
        DataFrame avec les colonnes numériques castées en DoubleType.
    """
    # Les colonnes non numériques sont conservées telles quelles
    non_numeric = [c for c in df_filosofi_raw.columns if c not in FILOSOFI_NUMERIC_COLS]

    numeric_exprs = [
        F.expr(f"try_cast(regexp_replace(`{c}`, ',', '.') as double)").alias(c)
        for c in FILOSOFI_NUMERIC_COLS
        if c in df_filosofi_raw.columns
    ]

    return df_filosofi_raw.select(
        *[F.col(c) for c in non_numeric],
        *numeric_exprs,
    )


def filter_filosofi(df_filosofi_typed: "DataFrame") -> "DataFrame":
    """Filtre les carreaux Filosofi valides.

    Conserve uniquement les carreaux certifiés (I_est_cr == 1.0)
    avec population et revenus positifs, et coordonnées disponibles.

    Args:
        df_filosofi_typed: DataFrame avec colonnes numériques castées.

    Returns:
        DataFrame Filosofi filtré.
    """
    return df_filosofi_typed.filter(
        (F.col("I_est_cr") == 1.0)
        & F.col("Ind").isNotNull() & (F.col("Ind") > 0)
        & F.col("Ind_snv").isNotNull() & (F.col("Ind_snv") > 0)
        & F.col("X_c").isNotNull()
        & F.col("Y_c").isNotNull()
    )

# ===== Cell 033 | markdown =====
# ### 2.5 – Feature engineering
#
# Cette sous-étape crée des variables utiles au scoring, par exemple la part de population cible padel et un score revenu normalisé.
#
# Le passage est important car il transforme des colonnes brutes en indicateurs directement exploitables pour la décision.

# ===== Cell 034 | code =====
df_filosofi_typed = cast_filosofi_numeric(df_filosofi_raw)
df_filosofi_clean = filter_filosofi(df_filosofi_typed)
print(f'Lignes apres nettoyage : {df_filosofi_clean.count()}')
df_filosofi_clean.select('IdINSPIRE', 'code_commune_insee', 'I_est_cr', 'Ind', 'Ind_snv', 'X_c', 'Y_c').show(10, truncate=False)

# ===== Cell 035 | markdown =====
# ### 2.4 – Conversion de projection : EPSG:3035 → WGS84
#
# Les coordonnées Filosofi sont en *Lambert Equal Area* (EPSG:3035), pas en GPS standard. On utilise une `pandas_udf` vectorisée avec `pyproj` pour la conversion — beaucoup plus rapide qu'une UDF Python classique ligne par ligne.

# ===== Cell 036 | code =====
def convert_epsg3035_to_wgs84(df_filosofi_clean: "DataFrame") -> "DataFrame":
    """Convertit les coordonnées EPSG:3035 vers WGS84 via Pandas UDF vectorisée.

    Utilise pyproj.Transformer dans un Pandas UDF Arrow pour éviter la
    conversion ligne par ligne. Le Transformer est mis en cache au niveau
    worker pour éviter une réinstanciation à chaque batch.

    Args:
        df_filosofi_clean: DataFrame avec X_c, Y_c en EPSG:3035.

    Returns:
        DataFrame enrichi de Longitude et Latitude en WGS84, filtré sur
        la France métropolitaine.
    """
    import importlib
    import subprocess
    import sys
    import pandas as pd

    if importlib.util.find_spec("pyproj") is None:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyproj"])

    @F.pandas_udf("struct<Longitude:double,Latitude:double>")
    def epsg3035_to_wgs84(x_series: pd.Series, y_series: pd.Series) -> pd.DataFrame:
        from pyproj import Transformer
        if not hasattr(epsg3035_to_wgs84, "_transformer"):
            epsg3035_to_wgs84._transformer = Transformer.from_crs(
                "EPSG:3035", "EPSG:4326", always_xy=True
            )
        lon, lat = epsg3035_to_wgs84._transformer.transform(
            x_series.values, y_series.values
        )
        return pd.DataFrame({"Longitude": lon, "Latitude": lat})

    return (
        df_filosofi_clean
        .withColumn("wgs84", epsg3035_to_wgs84(F.col("X_c"), F.col("Y_c")))
        .withColumn("Longitude", F.col("wgs84.Longitude"))
        .withColumn("Latitude", F.col("wgs84.Latitude"))
        .drop("wgs84")
        .filter(
            F.col("Latitude").between(41.0, 51.5)
            & F.col("Longitude").between(-5.5, 10.5)
        )
    )

# ===== Cell 037 | markdown =====
# Après conversion, on filtre les carreaux hors France métropolitaine (lat ∈ [41, 51.5], lon ∈ [-5.5, 10.5]).

# ===== Cell 038 | code =====
df_filosofi_geo = convert_epsg3035_to_wgs84(df_filosofi_clean)
print(f'Lignes apres conversion GPS + filtre geo : {df_filosofi_geo.count()}')
df_filosofi_geo.select('IdINSPIRE', 'Longitude', 'Latitude').show(10, truncate=False)

# ===== Cell 039 | markdown =====
# ### 2.5 – Feature engineering
#
# On calcule deux indicateurs dérivés :
# - `part_cible_padel` = (18-24 ans + 25-39 ans) / population totale du carreau
# - `score_revenu` = normalisation min-max du revenu médian dans [0, 1]
#
# On extrait aussi le `code_departement` depuis le code commune INSEE (avec fallback sur l'identifiant du carreau).

# ===== Cell 040 | code =====
def engineer_filosofi_features(df_filosofi_geo: "DataFrame") -> "DataFrame":
    """Calcule les features métier Filosofi et extrait code_departement.

    - part_cible_padel = (Ind_18_24 + Ind_25_39) / Ind
    - score_revenu = normalisation Min-Max de Ind_snv dans [0, 1]
    - code_departement dérivé depuis code_commune_insee (Corse, DOM, métropole)

    Args:
        df_filosofi_geo: DataFrame avec coordonnées WGS84.

    Returns:
        DataFrame avec features métier et code_departement.
    """
    # Part de la population cible Padel (18-39 ans)
    df_filosofi_feat = df_filosofi_geo.withColumn(
        "part_cible_padel",
        (
            F.coalesce(F.col("Ind_18_24"), F.lit(0.0))
            + F.coalesce(F.col("Ind_25_39"), F.lit(0.0))
        ) / F.col("Ind"),
    )

    # Normalisation Min-Max revenus (1 seule action collect)
    stats_row = df_filosofi_feat.agg(
        F.min("Ind_snv").alias("min_revenu"),
        F.max("Ind_snv").alias("max_revenu"),
    ).collect()[0]

    min_revenu = float(stats_row["min_revenu"] or 0.0)
    max_revenu = float(stats_row["max_revenu"] or 0.0)

    if max_revenu > min_revenu:
        df_filosofi_feat = df_filosofi_feat.withColumn(
            "score_revenu",
            (F.col("Ind_snv") - F.lit(min_revenu)) / F.lit(max_revenu - min_revenu),
        )
    else:
        df_filosofi_feat = df_filosofi_feat.withColumn("score_revenu", F.lit(0.0))

    # Extraction robuste du code département depuis le code commune INSEE
    code_commune_norm = F.upper(F.trim(F.col("code_commune_insee")))

    dep_from_commune = (
        F.when(code_commune_norm.rlike("^(2A|2B)"), F.regexp_extract(code_commune_norm, "^(2A|2B)", 1))
        .when(code_commune_norm.rlike("^(97[1-6])"), F.regexp_extract(code_commune_norm, "^(97[1-6])", 1))
        .otherwise(F.regexp_extract(code_commune_norm, "^(0[1-9]|[1-8][0-9]|9[0-5])", 1))
    )

    dep_regex_1 = F.regexp_extract(
        F.col("IdINSPIRE"),
        r"(?:^|[_-])((?:0[1-9]|[1-8][0-9]|9[0-5]|2A|2B))(?:[_-]|$)",
        1,
    )
    dep_regex_2 = F.regexp_extract(F.col("IdINSPIRE"), r"^FR(?:MET|DOM)?([0-9A-B]{2,3})", 1)

    df_filosofi_feat = df_filosofi_feat.withColumn(
        "code_departement",
        F.when(F.length(dep_from_commune) > 0, dep_from_commune)
        .when(F.length(dep_regex_1) > 0, dep_regex_1)
        .when(F.length(dep_regex_2) > 0, F.substring(dep_regex_2, 1, 2))
        .otherwise(F.lit(None).cast(StringType())),
    )

    # Filtrage final départements valides (métropole + Corse + DOM)
    return df_filosofi_feat.filter(
        F.col("code_departement").rlike("^(0[1-9]|[1-8][0-9]|9[0-5]|2A|2B|97[1-6])$")
    )

# ===== Cell 041 | markdown =====
# Sauvegarde en Parquet partitionné par département — ça accélère les jointures des étapes suivantes.

# ===== Cell 042 | code =====
OUTPUT_FILOSOFI = '/home/jovyan/work/data/output/filosofi_clean/'
df_filosofi_final = engineer_filosofi_features(df_filosofi_geo)
(
    df_filosofi_final.write
    .mode('overwrite')
    .partitionBy('code_departement')
    .parquet(OUTPUT_FILOSOFI)
)
print(f'Filosofi sauvegarde dans : {OUTPUT_FILOSOFI}')
print(f'Carreaux finaux : {df_filosofi_final.count()}')
df_filosofi_final.select(
    'IdINSPIRE', 'code_commune_insee', 'code_departement',
    'Ind', 'Ind_snv', 'part_cible_padel', 'score_revenu', 'Longitude', 'Latitude'
).show(10, truncate=False)

# ===== Cell 043 | markdown =====
# ---
# ## 3 – Clubs existants (RES + SIRENE)
#
# Ici, on reconstruit la concurrence réelle en fusionnant deux sources, puis en retirant les doublons.
#
# Exemple réel observé: 281 clubs RES, 606 clubs SIRENE, puis 831 clubs dédupliqués.

# ===== Cell 044 | markdown =====
# Dans cette partie, l’objectif est de reconstruire la concurrence réelle. Deux sources sont fusionnées, puis les doublons sont retirés. Sans cette étape, la concurrence serait surestimée et le score final serait moins juste.

# ===== Cell 045 | markdown =====
# ### 3.1 – Source RES
#
# Cette sous-étape charge la source RES, qui décrit les équipements sportifs. Elle apporte une base officielle des clubs et infrastructures.
#
# Dans les sorties, il faut vérifier la présence des coordonnées et des types d'équipement pour préparer la fusion avec SIRENE.

# ===== Cell 046 | code =====
def load_res_padel(spark: SparkSession, equip_path: str, install_path: str) -> "DataFrame":
    """Charge et prépare les données RES (Recensement des Équipements Sportifs).

    Filtre exclusivement sur les pistes de Padel. Joint les équipements
    aux installations pour récupérer les coordonnées GPS.

    Args:
        spark: Session Spark active.
        equip_path: Chemin CSV des équipements RES (séparateur ';').
        install_path: Chemin CSV des installations RES (séparateur ';').

    Returns:
        DataFrame standardisé avec colonnes Nom, Type, Commune,
        DepartementCode, Latitude, Longitude, Nombre_de_courts, Source.
    """
    df_res_equip = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("inferSchema", False)
        .csv(equip_path)
        .filter(F.col("type").rlike("(?i)padel"))
    )

    df_res_padel_counts = df_res_equip.groupBy("installation_numero").agg(
        F.count("*").alias("Nombre_de_courts"),
        F.first("type").alias("Type"),
    )

    df_res_inst = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("inferSchema", False)
        .csv(install_path)
        # Coordonnées GPS depuis la colonne 'coordonnees' (lat,lon)
        .withColumn("Latitude", F.split("coordonnees", ",")[0].cast(DoubleType()))
        .withColumn("Longitude", F.split("coordonnees", ",")[1].cast(DoubleType()))
        .filter(
            F.col("dep_code").rlike("^(0[1-9]|[1-8][0-9]|9[0-5]|2A|2B)$")
            & F.col("Latitude").isNotNull()
            & F.col("Longitude").isNotNull()
            & F.col("Latitude").between(41.0, 51.5)
            & F.col("Longitude").between(-5.0, 10.0)
        )
    )

    df_res = df_res_padel_counts.join(
        df_res_inst,
        df_res_padel_counts["installation_numero"] == df_res_inst["numero"],
        how="inner",
    )

    return df_res.select(
        df_res_inst["nom"].alias("Nom"),
        df_res_padel_counts["Type"],
        df_res_inst["commune"].alias("Commune"),
        df_res_inst["dep_code"].alias("DepartementCode"),
        df_res_inst["Latitude"],
        df_res_inst["Longitude"],
        df_res_padel_counts["Nombre_de_courts"].cast(IntegerType()),
        F.lit("RES").alias("Source"),
    )

# ===== Cell 047 | markdown =====
# ### 3.2 – Source SIRENE
#
# Cette sous-étape charge SIRENE, qui complète RES avec des structures privées. Le rôle de SIRENE est d'améliorer la couverture terrain.
#
# Illustration réelle observée: 606 clubs identifiés dans cette source après filtrage.

# ===== Cell 048 | code =====
df_res_std = load_res_padel(
    spark,
    equip_path='/home/jovyan/work/data/data-es-equipement.csv',
    install_path='/home/jovyan/work/data/data-es-installation.csv',
)
print(f'Clubs RES Padel : {df_res_std.count()}')
df_res_std.show(5, truncate=False)

# ===== Cell 049 | markdown =====
# ### 3.3 – Déduplication
#
# Les deux sources peuvent contenir le même club. Cette étape supprime les doublons pour éviter de surestimer la concurrence.
#
# Illustration réelle observée: 281 clubs RES + 606 clubs SIRENE, puis 831 clubs après déduplication.

# ===== Cell 050 | code =====
def load_sirene_padel(spark: SparkSession, etab_path: str, ul_path: str) -> "DataFrame":
    """Charge et prépare les données SIRENE filtrées Padel.

    Filtre sur l'activité sportive (93.*), l'état actif ('A') et le mot
    'padel' dans les dénominations. Convertit les coordonnées Lambert 93
    (EPSG:2154) vers WGS84 via Pandas UDF.

    Args:
        spark: Session Spark active.
        etab_path: Chemin Parquet StockEtablissement.
        ul_path: Chemin Parquet StockUniteLegale.

    Returns:
        DataFrame standardisé avec les mêmes colonnes que load_res_padel.
    """
    import pandas as pd

    df_sirene_raw = spark.read.parquet(etab_path)

    df_sirene_lambert = (
        df_sirene_raw
        .filter(
            F.col("activitePrincipaleEtablissement").startswith("93.")
            & (F.col("etatAdministratifEtablissement") == "A")
            & (F.col("coordonneeLambertAbscisseEtablissement") != "[ND]")
            & (F.col("coordonneeLambertOrdonneeEtablissement") != "[ND]")
            & F.col("coordonneeLambertAbscisseEtablissement").isNotNull()
            & F.col("coordonneeLambertOrdonneeEtablissement").isNotNull()
        )
        .withColumn(
            "lambert_x",
            F.col("coordonneeLambertAbscisseEtablissement").cast(DoubleType()),
        )
        .withColumn(
            "lambert_y",
            F.col("coordonneeLambertOrdonneeEtablissement").cast(DoubleType()),
        )
    )

    # Pandas UDF Arrow pour conversion EPSG:2154 -> WGS84 (vectorisée)
    from pyproj import Transformer as _Transformer
    _lambert_transformer = _Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)

    @F.pandas_udf("struct<Longitude:double,Latitude:double>")
    def lambert93_to_wgs84(x: pd.Series, y: pd.Series) -> pd.DataFrame:
        lon, lat = _lambert_transformer.transform(x.values, y.values)
        return pd.DataFrame({"Longitude": lon, "Latitude": lat})

    df_sirene_converted = (
        df_sirene_lambert
        .withColumn("wgs84", lambert93_to_wgs84(F.col("lambert_x"), F.col("lambert_y")))
        .withColumn("Longitude", F.col("wgs84.Longitude"))
        .withColumn("Latitude", F.col("wgs84.Latitude"))
        .drop("wgs84")
        .filter(
            F.col("Latitude").between(41.0, 51.5)
            & F.col("Longitude").between(-5.0, 10.0)
        )
    )

    df_ul = spark.read.parquet(ul_path).select(
        "siren", "denominationUniteLegale", "sigleUniteLegale"
    )
    df_sirene_join = df_sirene_converted.join(df_ul, on="siren", how="left")

    # Résolution du nom prioritaire contenant 'padel'
    nom_principal = F.coalesce(
        F.when(F.col("enseigne1Etablissement").rlike("(?i)padel"), F.col("enseigne1Etablissement")),
        F.when(F.col("denominationUsuelleEtablissement").rlike("(?i)padel"), F.col("denominationUsuelleEtablissement")),
        F.when(F.col("denominationUniteLegale").rlike("(?i)padel"), F.col("denominationUniteLegale")),
        F.col("denominationUniteLegale"),
        F.col("sigleUniteLegale"),
        F.col("enseigne1Etablissement"),
        F.col("denominationUsuelleEtablissement"),
        F.concat(F.lit("Club de Sport de "), F.col("libelleCommuneEtablissement")),
    )

    return (
        df_sirene_join
        .select(
            nom_principal.alias("Nom"),
            F.lit("Club de Padel").alias("Type"),
            F.col("libelleCommuneEtablissement").alias("Commune"),
            F.substring("codeCommuneEtablissement", 1, 2).alias("DepartementCode"),
            F.col("Latitude"),
            F.col("Longitude"),
            F.lit(None).cast(IntegerType()).alias("Nombre_de_courts"),
            F.lit("SIRENE").alias("Source"),
        )
        # Filtre sémantique strict : conserver uniquement les entités avec 'padel' dans le nom
        .filter(F.col("Nom").rlike("(?i)padel"))
    )

# ===== Cell 051 | markdown =====
# ### 4.1 – Parsing OSM
#
# Cette sous-étape lit le fichier OpenStreetMap brut pour extraire les objets utiles, notamment les arrêts de transport et les axes routiers.
#
# Le résultat attendu est une table intermédiaire propre, prête à être transformée en indicateurs d'accessibilité.

# ===== Cell 052 | code =====
df_sirene_std = load_sirene_padel(
    spark,
    etab_path='/home/jovyan/work/data/StockEtablissement_utf8.parquet',
    ul_path='/home/jovyan/work/data/StockUniteLegale_utf8.parquet',
)
print(f'Clubs SIRENE Padel : {df_sirene_std.count()}')
df_sirene_std.show(5, truncate=False)

# ===== Cell 053 | markdown =====
# ### 4.2 – Ingestion Spark et schéma explicite
#
# Cette cellule charge les extractions OSM dans Spark avec un schéma défini. L'objectif est d'éviter les erreurs de typage et d'obtenir des colonnes stables.
#
# Un schéma explicite rend l'étape reproductible et facilite les jointures suivantes.

# ===== Cell 054 | code =====
def deduplicate_padel_clubs(
    df_res_std: "DataFrame",
    df_sirene_std: "DataFrame",
    output_path: str,
) -> "DataFrame":
    """Fusionne RES et SIRENE, déduplique par GPS arrondi à 3 décimales.

    La déduplication priorise les lignes RES (seule source avec Nombre_de_courts).
    Sauvegarde en Parquet partitionné par DepartementCode.

    Args:
        df_res_std: DataFrame RES standardisé.
        df_sirene_std: DataFrame SIRENE standardisé.
        output_path: Chemin de sortie Parquet.

    Returns:
        DataFrame dédupliqué des clubs Padel.
    """
    from pyspark.sql.window import Window

    df_combined = df_res_std.unionByName(df_sirene_std, allowMissingColumns=True)

    window_dedup = (
        Window
        .partitionBy(
            F.round("Latitude", 3).alias("lat_approx"),
            F.round("Longitude", 3).alias("lon_approx"),
        )
        .orderBy(F.col("Nombre_de_courts").desc_nulls_last())
    )

    df_dedup = (
        df_combined
        .withColumn("lat_approx", F.round("Latitude", 3))
        .withColumn("lon_approx", F.round("Longitude", 3))
        .withColumn("row_num", F.row_number().over(
            Window.partitionBy("lat_approx", "lon_approx")
            .orderBy(F.col("Nombre_de_courts").desc_nulls_last())
        ))
        .filter(F.col("row_num") == 1)
        .drop("lat_approx", "lon_approx", "row_num")
        .filter(F.col("DepartementCode").rlike("^(0[1-9]|[1-8][0-9]|9[0-5]|2A|2B)$"))
    )

    (
        df_dedup.write
        .mode("overwrite")
        .partitionBy("DepartementCode")
        .parquet(output_path)
    )

    print(f"Sauvegarde Padel clubs -> {output_path}")
    print(f"Clubs Padel dedupliques : {df_dedup.count()}")
    return df_dedup

# ===== Cell 055 | markdown =====
# ### 4.3 – Indicateurs d'accessibilité par carreau
#
# Ici, la donnée transport et route est transformée en indicateurs numériques par carreau. Cette transformation rend la mobilité comparable entre zones.
#
# Illustration réelle observée: 1 809 025 carreaux scorés en accessibilité.

# ===== Cell 056 | code =====
OUTPUT_CONCURRENCE = '/home/jovyan/work/data/output/concurrence_padel/'
df_clubs_dedup = deduplicate_padel_clubs(df_res_std, df_sirene_std, OUTPUT_CONCURRENCE)
df_clubs_dedup.groupBy('Source').count().show()
df_clubs_dedup.show(10, truncate=False)

# ===== Cell 057 | markdown =====
# ---
# ## 4 – Accessibilité (OpenStreetMap)
#
# Cette étape transforme la donnée cartographique brute en indicateurs simples, comme la densité des arrêts de transport et la proximité des axes routiers.
#
# Exemple réel observé: 1 809 025 carreaux scorés sur l'accessibilité.

# ===== Cell 058 | markdown =====
# OpenStreetMap apporte ici une lecture simple de l’accessibilité. La géométrie brute est transformée en indicateurs compréhensibles, comme la proximité des axes et la densité des arrêts. Ces indicateurs complètent la partie économique et démographique.

# ===== Cell 059 | markdown =====
# ### 4.4 – Sauvegarde
#
# Cette sous-étape enregistre les résultats d'accessibilité dans un format réutilisable. Cela évite de relancer les calculs lourds à chaque test.
#
# Dans un rapport, cette étape justifie la séparation entre calcul coûteux et exploitation analytique.

# ===== Cell 060 | code =====
"""
Section 4.1  Parsing OSM PBF via pyosmium (ULTRA optimisé).
- Sous-processus dédié (kernel Jupyter protégé).
- Écriture streaming + fichiers temporaires atomiques (.tmp -> .csv).
- Parsing en 2 passes :
  1) arrêts TC (nodes) sans index de localisation
  2) axes routiers (ways) avec index flex_mem (plus rapide)
- Visualisateur d'avancement live dans le notebook.
"""
import os
import importlib.util
import subprocess
import sys
import textwrap
import time
from IPython.display import clear_output

input_pbf = "/home/jovyan/work/data/france-260319.osm.pbf"
output_transport_csv = "/home/jovyan/work/data/output/osm_transport_stops.csv"
output_road_csv = "/home/jovyan/work/data/output/osm_road_axes.csv"
parser_script_path = "/home/jovyan/work/data/output/osm_parse_streaming.py"

# Installation automatique si nécessaire (runtime notebook)
if importlib.util.find_spec("osmium") is None:
    print("ðŸ“¦ Installation de pyosmium...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "osmium"])

if importlib.util.find_spec("numpy") is None:
    print("ðŸ“¦ Installation de numpy...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "numpy"])

if not os.path.exists(input_pbf):
    raise FileNotFoundError(f"Fichier PBF introuvable: {input_pbf}")

# Re-parse uniquement si les CSV intermédiaires n'existent pas
if (not os.path.exists(output_transport_csv)) or (not os.path.exists(output_road_csv)):
    os.makedirs("/home/jovyan/work/data/output", exist_ok=True)

    parser_code = textwrap.dedent("""
    import csv
    import os
    import sys

    pbf_path = sys.argv[1]
    transport_csv_path = sys.argv[2]
    road_csv_path = sys.argv[3]

    transport_tmp = transport_csv_path + ".tmp"
    road_tmp = road_csv_path + ".tmp"

    class OSMAccessibilityExtractor:
        def run(self):
            import osmium
            import numpy as np

            os.makedirs(os.path.dirname(transport_csv_path), exist_ok=True)

            # -----------------------------
            # PASS 1 : Stops transport (nodes)
            # -----------------------------
            class TransportStopsHandler(osmium.SimpleHandler):
                def __init__(self):
                    super().__init__()
                    # Contrainte projet : imports dans le handler
                    import osmium as osmium_local
                    import numpy as numpy_local
                    self.osmium_local = osmium_local
                    self.numpy_local = numpy_local

                    self.f_tc = open(transport_tmp, "w", newline="", encoding="utf-8")
                    self.writer_tc = csv.DictWriter(
                        self.f_tc,
                        fieldnames=["osm_id", "type_transport", "latitude", "longitude"]
                    )
                    self.writer_tc.writeheader()

                    self.transport_count = 0
                    self.node_seen = 0

                def node(self, node):
                    self.node_seen += 1
                    if self.node_seen % 2_000_000 == 0:
                        self.f_tc.flush()
                        print(f"PROGRESS|pass1|{self.node_seen}|{self.transport_count}", flush=True)

                    if not node.location.valid():
                        return

                    tags = node.tags
                    type_transport = None
                    if tags.get("highway") == "bus_stop":
                        type_transport = "bus_stop"
                    elif tags.get("railway") == "station":
                        type_transport = "station"
                    elif tags.get("railway") == "tram_stop":
                        type_transport = "tram_stop"
                    elif tags.get("railway") == "subway_entrance":
                        type_transport = "subway_entrance"
                    elif tags.get("public_transport") == "stop_position":
                        type_transport = "stop_position"

                    if type_transport is not None:
                        self.writer_tc.writerow({
                            "osm_id": int(node.id),
                            "type_transport": type_transport,
                            "latitude": float(node.location.lat),
                            "longitude": float(node.location.lon),
                        })
                        self.transport_count += 1

                def close(self):
                    self.f_tc.flush()
                    self.f_tc.close()

            print("STATUS|pass1_start", flush=True)
            h1 = TransportStopsHandler()
            try:
                # locations=False : plus rapide pour les nodes
                h1.apply_file(pbf_path, locations=False)
            finally:
                h1.close()
            print(f"STATUS|pass1_done|{h1.transport_count}", flush=True)

            # -----------------------------
            # PASS 2 : Axes routiers (ways)
            # -----------------------------
            class RoadAxesHandler(osmium.SimpleHandler):
                def __init__(self):
                    super().__init__()
                    # Contrainte projet : imports dans le handler
                    import osmium as osmium_local
                    import numpy as numpy_local
                    self.osmium_local = osmium_local
                    self.np = numpy_local

                    self.f_rd = open(road_tmp, "w", newline="", encoding="utf-8")
                    self.writer_rd = csv.DictWriter(
                        self.f_rd,
                        fieldnames=["osm_id", "type_voie", "latitude_centroid", "longitude_centroid"]
                    )
                    self.writer_rd.writeheader()

                    self.road_count = 0
                    self.way_seen = 0
                    self.skipped_ways_without_nodes = 0

                def way(self, way):
                    self.way_seen += 1
                    if self.way_seen % 300_000 == 0:
                        self.f_rd.flush()
                        print(f"PROGRESS|pass2|{self.way_seen}|{self.road_count}", flush=True)

                    highway = way.tags.get("highway")
                    if highway not in {"motorway", "trunk", "primary"}:
                        return

                    lat_sum = 0.0
                    lon_sum = 0.0
                    count = 0
                    for n in way.nodes:
                        if n.location.valid():
                            lat_sum += float(n.location.lat)
                            lon_sum += float(n.location.lon)
                            count += 1

                    if count == 0:
                        self.skipped_ways_without_nodes += 1
                        return

                    lat_c = float(self.np.float64(lat_sum / count))
                    lon_c = float(self.np.float64(lon_sum / count))

                    self.writer_rd.writerow({
                        "osm_id": int(way.id),
                        "type_voie": highway,
                        "latitude_centroid": lat_c,
                        "longitude_centroid": lon_c,
                    })
                    self.road_count += 1

                def close(self):
                    self.f_rd.flush()
                    self.f_rd.close()

            print("STATUS|pass2_start", flush=True)
            h2 = RoadAxesHandler()
            try:
                # idx=flex_mem : rapide si RAM suffisante
                h2.apply_file(pbf_path, locations=True, idx="flex_mem")
            finally:
                h2.close()

            print(f"STATUS|pass2_done|{h2.road_count}|{h2.skipped_ways_without_nodes}", flush=True)

            # Remplacement atomique des sorties
            os.replace(transport_tmp, transport_csv_path)
            os.replace(road_tmp, road_csv_path)

            print(f"DONE|{h1.transport_count}|{h2.road_count}|{h2.skipped_ways_without_nodes}", flush=True)
            print(f"PATH|transport|{transport_csv_path}", flush=True)
            print(f"PATH|roads|{road_csv_path}", flush=True)

    if __name__ == "__main__":
        OSMAccessibilityExtractor().run()
    """)

    with open(parser_script_path, "w", encoding="utf-8") as f_script:
        f_script.write(parser_code)

    cmd = [sys.executable, parser_script_path, input_pbf, output_transport_csv, output_road_csv]
    print("ðŸš€ Lancement du parsing OSM ULTRA optimisé (2 passes + progress live)...")

    state = {
        "phase": "init",
        "pass1_seen": 0,
        "pass1_kept": 0,
        "pass2_seen": 0,
        "pass2_kept": 0,
        "skipped_ways": 0,
        "last_lines": []
    }
    start_ts = time.time()

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    try:
        for raw_line in process.stdout:
            line = raw_line.strip()
            if not line:
                continue

            state["last_lines"].append(line)
            state["last_lines"] = state["last_lines"][-8:]

            if line.startswith("STATUS|pass1_start"):
                state["phase"] = "PASS 1/2 - Stops transport"
            elif line.startswith("STATUS|pass2_start"):
                state["phase"] = "PASS 2/2 - Axes routiers"
            elif line.startswith("PROGRESS|pass1|"):
                _, _, seen, kept = line.split("|")
                state["pass1_seen"] = int(seen)
                state["pass1_kept"] = int(kept)
                state["phase"] = "PASS 1/2 - Stops transport"
            elif line.startswith("PROGRESS|pass2|"):
                _, _, seen, kept = line.split("|")
                state["pass2_seen"] = int(seen)
                state["pass2_kept"] = int(kept)
                state["phase"] = "PASS 2/2 - Axes routiers"
            elif line.startswith("STATUS|pass2_done|"):
                parts = line.split("|")
                state["pass2_kept"] = int(parts[2])
                state["skipped_ways"] = int(parts[3])
            elif line.startswith("DONE|"):
                _, tc, rd, skipped = line.split("|")
                state["pass1_kept"] = int(tc)
                state["pass2_kept"] = int(rd)
                state["skipped_ways"] = int(skipped)
                state["phase"] = "TERMINÉ"

            elapsed = int(time.time() - start_ts)
            clear_output(wait=True)
            print("ðŸ“¡ Visualiseur d'avancement OSM")
            print(f"Phase              : {state['phase']}")
            print(f"Temps écoulé       : {elapsed//3600:02d}:{(elapsed%3600)//60:02d}:{elapsed%60:02d}")
            print(f"Nodes lus (pass1)  : {state['pass1_seen']:,}")
            print(f"Arrêts extraits    : {state['pass1_kept']:,}")
            print(f"Ways lus (pass2)   : {state['pass2_seen']:,}")
            print(f"Axes extraits      : {state['pass2_kept']:,}")
            print(f"Ways sans nodes    : {state['skipped_ways']:,}")
            print("\nDerniers logs:")
            for msg in state["last_lines"]:
                print(f" - {msg}")

        return_code = process.wait()
        if return_code != 0:
            raise subprocess.CalledProcessError(return_code, cmd)

        clear_output(wait=True)
        print("  Parsing OSM terminé")
        print(f" - Arrêts TC   : {state['pass1_kept']:,}")
        print(f" - Axes routes : {state['pass2_kept']:,}")
        print(f" - Ways ignorés: {state['skipped_ways']:,}")
        print(f" - CSV transport: {output_transport_csv}")
        print(f" - CSV routes   : {output_road_csv}")
    finally:
        if process.stdout:
            process.stdout.close()
else:
    print(" CSV OSM intermédiaires déjÃ  présents, parsing ignoré.")
    print(f" - {output_transport_csv}")
    print(f" - {output_road_csv}")

# ===== Cell 061 | markdown =====
# ### 5.1 – Chargement et normalisation
#
# Google Trends est chargé puis harmonisé. Le but est de rendre cette source compatible avec les autres tables du pipeline.
#
# Cette étape évite les différences d'écriture ou de format qui cassent les jointures.

# ===== Cell 062 | code =====
"""
Section 4.2  Ingestion Spark des CSV OSM avec schémas explicites.
- Chargement des arrêts TC et axes routiers extraits par pyosmium.
- Schémas stricts (pas d'inferSchema).
- Nettoyage minimal des coordonnées nulles.
- Compatible avec un kernel redémarré (sans exécuter la section 4.1).
"""
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Fallback Spark si kernel redémarré
if "spark" not in globals() or spark is None:
    spark = SparkSession.builder \
        .appName("PadelSpot_Step4_Accessibilite") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

# Fallback chemins CSV si la section 4.1 n'a pas été exécutée dans ce kernel
if "output_transport_csv" not in globals():
    output_transport_csv = "/home/jovyan/work/data/output/osm_transport_stops.csv"
if "output_road_csv" not in globals():
    output_road_csv = "/home/jovyan/work/data/output/osm_road_axes.csv"

if not os.path.exists(output_transport_csv) or not os.path.exists(output_road_csv):
    raise FileNotFoundError(
        "CSV OSM introuvables. Vérifie que les fichiers existent dans /home/jovyan/work/data/output/."
    )

transport_schema = StructType([
    StructField("osm_id", LongType(), True),
    StructField("type_transport", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])

road_schema = StructType([
    StructField("osm_id", LongType(), True),
    StructField("type_voie", StringType(), True),
    StructField("latitude_centroid", DoubleType(), True),
    StructField("longitude_centroid", DoubleType(), True),
])

df_osm_transport = spark.read \
    .option("header", True) \
    .schema(transport_schema) \
    .csv(output_transport_csv) \
    .filter(
        F.col("latitude").isNotNull() &
        F.col("longitude").isNotNull()
    )

df_osm_roads = spark.read \
    .option("header", True) \
    .schema(road_schema) \
    .csv(output_road_csv) \
    .filter(
        F.col("latitude_centroid").isNotNull() &
        F.col("longitude_centroid").isNotNull()
    )

print(f"ðŸšŒ ArrÃªts TC chargés : {df_osm_transport.count()}")
print(f"ðŸ›£ï¸ Axes routiers chargés : {df_osm_roads.count()}")
df_osm_transport.show(5, truncate=False)
df_osm_roads.show(5, truncate=False)

# ===== Cell 063 | markdown =====
# ### 4.3 – Indicateurs d'accessibilité par carreau
#
# Pour chaque carreau Filosofi, on calcule :
# - `densite_tc` : nombre d'arrêts TC dans un rayon de 1 km
# - `proximite_axe` : distance au tronçon routier principal le plus proche

# ===== Cell 064 | code =====
"""
Section 4.3  Calcul d'accessibilité par carreau INSEE.
- Charge les carreaux de l'étape 2 (filosofi_clean).
- Calcule densite_tc (arrêts TC à <=1 km).
- Calcule proximite_axe (distance minimale à un axe majeur).
- Utilise une Pandas UDF Haversine vectorisée (Arrow).
"""
import pandas as pd

@F.pandas_udf("double")
def haversine_km(lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
    # Calcul vectorisé Haversine en km
    import numpy as np
    r = 6371.0
    lat1_rad = np.radians(lat1.values)
    lon1_rad = np.radians(lon1.values)
    lat2_rad = np.radians(lat2.values)
    lon2_rad = np.radians(lon2.values)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2.0) ** 2
    c = 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))
    return pd.Series(r * c)

# Chargement des carreaux nettoyés (Étape 2)
df_carreaux = spark.read.parquet("/home/jovyan/work/data/output/filosofi_clean/") \
    .select(
        "IdINSPIRE", "code_departement", "code_commune_insee",
        "Latitude", "Longitude", "Ind", "Ind_snv",
        "part_cible_padel", "score_revenu"
    ) \
    .filter(F.col("Latitude").isNotNull() & F.col("Longitude").isNotNull())

# -------------------------
# 1) Densité transport (<= 1 km)
# -------------------------
bucket_deg_tc = 0.01  # ~1.1 km en latitude
df_tc_b = df_osm_transport \
    .select(
        F.col("osm_id").alias("osm_id_tc"),
        F.col("latitude").alias("lat_tc"),
        F.col("longitude").alias("lon_tc")
    ) \
    .withColumn("lat_b", F.floor(F.col("lat_tc") / F.lit(bucket_deg_tc)).cast("int")) \
    .withColumn("lon_b", F.floor(F.col("lon_tc") / F.lit(bucket_deg_tc)).cast("int"))

df_c_b = df_carreaux \
    .select(
        "IdINSPIRE",
        F.col("Latitude").alias("lat_cell"),
        F.col("Longitude").alias("lon_cell")
    ) \
    .withColumn("lat_b", F.floor(F.col("lat_cell") / F.lit(bucket_deg_tc)).cast("int")) \
    .withColumn("lon_b", F.floor(F.col("lon_cell") / F.lit(bucket_deg_tc)).cast("int"))

# Expansion des buckets voisins (3x3) pour jointure spatiale approximative
neighbors_tc = F.array(*[
    F.struct(
        (F.col("lat_b") + F.lit(dlat)).alias("lat_b_n"),
        (F.col("lon_b") + F.lit(dlon)).alias("lon_b_n")
    )
    for dlat in [-1, 0, 1]
    for dlon in [-1, 0, 1]
])

df_c_tc_candidates = df_c_b \
    .withColumn("nb", F.explode(neighbors_tc)) \
    .select(
        "IdINSPIRE", "lat_cell", "lon_cell",
        F.col("nb.lat_b_n").alias("lat_b_n"),
        F.col("nb.lon_b_n").alias("lon_b_n")
    )

df_tc_pairs = df_c_tc_candidates.join(
    df_tc_b,
    (df_c_tc_candidates["lat_b_n"] == df_tc_b["lat_b"]) &
    (df_c_tc_candidates["lon_b_n"] == df_tc_b["lon_b"]),
    how="left"
)

df_tc_pairs = df_tc_pairs.withColumn(
    "dist_tc_km",
    haversine_km(
        F.col("lat_cell"), F.col("lon_cell"),
        F.col("lat_tc"), F.col("lon_tc")
    )
 )

df_densite_tc = df_tc_pairs.filter(F.col("dist_tc_km") <= 1.0) \
    .groupBy("IdINSPIRE") \
    .agg(F.countDistinct("osm_id_tc").alias("densite_tc"))

# -------------------------
# 2) Proximité axe routier (distance min)
# -------------------------
bucket_deg_rd = 0.02  # bucket plus large pour limiter les cas sans candidats
df_rd_b = df_osm_roads \
    .select(
        F.col("osm_id").alias("osm_id_rd"),
        F.col("latitude_centroid").alias("lat_rd"),
        F.col("longitude_centroid").alias("lon_rd")
    ) \
    .withColumn("lat_b", F.floor(F.col("lat_rd") / F.lit(bucket_deg_rd)).cast("int")) \
    .withColumn("lon_b", F.floor(F.col("lon_rd") / F.lit(bucket_deg_rd)).cast("int"))

df_c_r_b = df_carreaux \
    .select(
        "IdINSPIRE",
        F.col("Latitude").alias("lat_cell"),
        F.col("Longitude").alias("lon_cell")
    ) \
    .withColumn("lat_b", F.floor(F.col("lat_cell") / F.lit(bucket_deg_rd)).cast("int")) \
    .withColumn("lon_b", F.floor(F.col("lon_cell") / F.lit(bucket_deg_rd)).cast("int"))

# Expansion voisins 5x5 (±2 buckets)
neighbors_rd = F.array(*[
    F.struct(
        (F.col("lat_b") + F.lit(dlat)).alias("lat_b_n"),
        (F.col("lon_b") + F.lit(dlon)).alias("lon_b_n")
    )
    for dlat in [-2, -1, 0, 1, 2]
    for dlon in [-2, -1, 0, 1, 2]
])

df_c_rd_candidates = df_c_r_b \
    .withColumn("nb", F.explode(neighbors_rd)) \
    .select(
        "IdINSPIRE", "lat_cell", "lon_cell",
        F.col("nb.lat_b_n").alias("lat_b_n"),
        F.col("nb.lon_b_n").alias("lon_b_n")
    )

df_rd_pairs = df_c_rd_candidates.join(
    df_rd_b,
    (df_c_rd_candidates["lat_b_n"] == df_rd_b["lat_b"]) &
    (df_c_rd_candidates["lon_b_n"] == df_rd_b["lon_b"]),
    how="left"
)

df_rd_pairs = df_rd_pairs.withColumn(
    "dist_axe_km",
    haversine_km(
        F.col("lat_cell"), F.col("lon_cell"),
        F.col("lat_rd"), F.col("lon_rd")
    )
 )

df_proximite_axe = df_rd_pairs \
    .groupBy("IdINSPIRE") \
    .agg(F.min("dist_axe_km").alias("proximite_axe"))

# -------------------------
# 3) Fusion indicateurs + score
# -------------------------
df_access = df_carreaux \
    .join(df_densite_tc, on="IdINSPIRE", how="left") \
    .join(df_proximite_axe, on="IdINSPIRE", how="left")

# Gestion valeurs manquantes
df_access = df_access.withColumn("densite_tc", F.coalesce(F.col("densite_tc"), F.lit(0)))

max_prox_row = df_access.agg(F.max("proximite_axe").alias("max_prox")).collect()[0]
max_prox_value = float(max_prox_row["max_prox"]) if max_prox_row["max_prox"] is not None else 50.0
df_access = df_access.withColumn(
    "proximite_axe",
    F.coalesce(F.col("proximite_axe"), F.lit(max_prox_value + 5.0))
)

# Normalisation Min-Max densité
stat_dens = df_access.agg(F.min("densite_tc").alias("min_d"), F.max("densite_tc").alias("max_d")).collect()[0]
min_d, max_d = float(stat_dens["min_d"]), float(stat_dens["max_d"])
if max_d > min_d:
    df_access = df_access.withColumn(
        "densite_tc_norm",
        (F.col("densite_tc") - F.lit(min_d)) / F.lit(max_d - min_d)
    )
else:
    df_access = df_access.withColumn("densite_tc_norm", F.lit(0.0))

# Normalisation Min-Max proximité
stat_prox = df_access.agg(F.min("proximite_axe").alias("min_p"), F.max("proximite_axe").alias("max_p")).collect()[0]
min_p, max_p = float(stat_prox["min_p"]), float(stat_prox["max_p"])
if max_p > min_p:
    df_access = df_access.withColumn(
        "proximite_axe_norm",
        (F.col("proximite_axe") - F.lit(min_p)) / F.lit(max_p - min_p)
    )
else:
    df_access = df_access.withColumn("proximite_axe_norm", F.lit(0.0))

# Formule finale
df_access = df_access.withColumn(
    "score_accessibilite",
    F.lit(0.6) * F.col("densite_tc_norm") + F.lit(0.4) * (F.lit(1.0) - F.col("proximite_axe_norm"))
)

print(f"ðŸ§® Carreaux scorés accessibilité : {df_access.count()}")
df_access.select(
    "IdINSPIRE", "code_departement", "densite_tc", "proximite_axe", "score_accessibilite"
).show(10, truncate=False)

# ===== Cell 065 | markdown =====
# ### 5.2 – Projection région → départements
#
# La source Trends est régionale, mais le pipeline travaille plus finement. Cette cellule projette donc la valeur régionale au niveau départemental.
#
# Illustration réelle observée: 96 départements couverts.

# ===== Cell 066 | code =====
"""
Section 4.4  Sauvegarde finale accessibilité.
- Sauvegarde en parquet partitionné par code_departement.
- Contrôle rapide des volumes et partitions.
"""
output_path_access = "/home/jovyan/work/data/output/accessibilite_clean/"

df_access.write \
    .mode("overwrite") \
    .partitionBy("code_departement") \
    .parquet(output_path_access)

print(f" Accessibilité sauvegardée dans : {output_path_access}")
print(f"ðŸ“Š Nombre total de carreaux accessibilité : {df_access.count()}")
print("ðŸ“¦ Top 20 départements par volume :")
df_access.groupBy("code_departement").count().orderBy(F.desc("count")).show(20, truncate=False)

# ===== Cell 067 | markdown =====
# ---
# ## 5 – Demande latente (Google Trends)
#
# Cette section ajoute un signal de demande. La donnée est d'abord régionale, puis elle est projetée vers les départements pour être compatible avec le reste du pipeline.
#
# Exemple réel observé: 96 départements couverts.

# ===== Cell 068 | markdown =====
# Google Trends ajoute un signal de demande. La difficulté est de rendre cette source compatible avec les autres données du pipeline. La transformation montre comment une information régionale peut être projetée et utilisée dans une analyse locale.

# ===== Cell 069 | markdown =====
# ### 5.1 – Chargement et normalisation
#
# Le fichier `geoMap.csv` est exporté depuis Google Trends. On normalise les régions (accents, tirets) et on impute les valeurs manquantes par la médiane nationale.

# ===== Cell 070 | code =====
# ============================================================
# ÉTAPE 5 – Demande latente Google Trends
# ============================================================

# Mapping région -> départements métropolitains (22 anciennes régions)
REGION_TO_DEPARTEMENTS: dict[str, list[str]] = {
    "alsace": ["67", "68"],
    "aquitaine": ["24", "33", "40", "47", "64"],
    "auvergne": ["03", "15", "43", "63"],
    "basse normandie": ["14", "50", "61"],
    "bourgogne": ["21", "58", "71", "89"],
    "bretagne": ["22", "29", "35", "56"],
    "centre": ["18", "28", "36", "37", "41", "45"],
    "champagne ardenne": ["08", "10", "51", "52"],
    "corse": ["2A", "2B"],
    "franche comte": ["25", "39", "70", "90"],
    "haute normandie": ["27", "76"],
    "ile de france": ["75", "77", "78", "91", "92", "93", "94", "95"],
    "languedoc roussillon": ["11", "30", "34", "48", "66"],
    "limousin": ["19", "23", "87"],
    "lorraine": ["54", "55", "57", "88"],
    "midi pyrenees": ["09", "12", "31", "32", "46", "65", "81", "82"],
    "nord pas de calais": ["59", "62"],
    "pays de la loire": ["44", "49", "53", "72", "85"],
    "picardie": ["02", "60", "80"],
    "poitou charentes": ["16", "17", "79", "86"],
    "provence alpes cote d azur": ["04", "05", "06", "13", "83", "84"],
    "rhone alpes": ["01", "07", "26", "38", "42", "69", "73", "74"],
}

# Alias de normalisation source -> canonique
REGION_ALIASES = {
    "centre val de loire": "centre",
}


def load_trends(spark: SparkSession, trends_path: str) -> "DataFrame":
    """Charge et normalise le fichier Google Trends régional.

    Normalise les noms de régions via expressions PySpark natives
    (regexp_replace chaîné, entièrement vectorisé, pas d'UDF Python).
    Impute les nulls par la médiane nationale (percentile_approx).

    Args:
        spark: Session Spark active.
        trends_path: Chemin du fichier geoMap.csv.

    Returns:
        DataFrame avec colonnes region_norm (str) et indice_trends (int).
    """
    trends_schema = StructType([
        StructField("region_raw", StringType(), True),
        StructField("indice_raw", StringType(), True),
    ])

    df_trends_raw = (
        spark.read
        .option("header", True)
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .schema(trends_schema)
        .csv(trends_path)
    )

    # Normalisation des accents français par regexp_replace chaîné
    # (couvre 100% des 22 régions du dataset, entièrement vectorisé via Catalyst)
    def _normalize_col(col_expr: "Column") -> "Column":
        c = F.lower(F.trim(col_expr))
        c = F.regexp_replace(c, r"[àâä]", "a")
        c = F.regexp_replace(c, r"[éèêë]", "e")
        c = F.regexp_replace(c, r"[îï]", "i")
        c = F.regexp_replace(c, r"[ôö]", "o")
        c = F.regexp_replace(c, r"[ùûü]", "u")
        c = F.regexp_replace(c, r"ç", "c")
        c = F.regexp_replace(c, r"[-_']", " ")
        c = F.regexp_replace(c, r"[^a-z0-9 ]+", " ")
        c = F.regexp_replace(c, r"\s+", " ")
        return F.trim(c)

    # Carte d'alias (petit volume -> map Spark natif)
    alias_map = F.create_map(
        *[x for kv in REGION_ALIASES.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    )

    df_trends_clean = (
        df_trends_raw
        .withColumn("region_norm_raw", _normalize_col(F.col("region_raw")))
        .withColumn(
            "indice_trends",
            F.regexp_extract(F.coalesce(F.col("indice_raw"), F.lit("")), r"(\d+)", 1)
            .cast(IntegerType()),
        )
        .withColumn(
            "region_norm",
            F.coalesce(alias_map[F.col("region_norm_raw")], F.col("region_norm_raw")),
        )
        .filter(F.col("region_norm") != "region")
    )

    # Imputation par médiane (1 seule action collect)
    median_row = df_trends_clean.agg(
        F.expr("percentile_approx(indice_trends, 0.5, 1000)").alias("median_indice")
    ).collect()[0]
    median_indice = int(median_row["median_indice"] or 50)

    print(f"Fichier Trends utilise : {trends_path}")
    print(f"Mediane nationale (imputation nulls) : {median_indice}")

    return (
        df_trends_clean
        .withColumn(
            "indice_trends",
            F.coalesce(F.col("indice_trends"), F.lit(median_indice)),
        )
        .select("region_raw", "region_norm", "indice_trends")
    ), median_indice


# ===== Cell 071 | markdown =====
# ### 5.3 – Jointure avec les carreaux Filosofi
#
# Cette cellule attache la demande Trends aux carreaux déjà préparés. Elle relie donc le signal de demande à l'échelle locale utilisée par le score final.
#
# Illustration réelle observée: 1 809 025 carreaux enrichis.

# ===== Cell 072 | code =====
import os

TRENDS_CANDIDATES = [
    '/home/jovyan/work/data/trends/geoMap.csv',
    '/home/jovyan/work/data/geoMap.csv',
]
trends_path = next((p for p in TRENDS_CANDIDATES if os.path.exists(p)), None)
if trends_path is None:
    raise FileNotFoundError('geoMap.csv introuvable.')

df_trends_clean, median_indice = load_trends(spark, trends_path)
print(f'Lignes Trends lues : {df_trends_clean.count()}')
df_trends_clean.show(30, truncate=False)

# ===== Cell 073 | markdown =====
# ### 6.1 – Normalisation et poids
#
# Les variables n'ont pas les mêmes unités. Cette étape les normalise et applique des poids pour construire une contribution comparable de chaque composante.
#
# Le lecteur doit retenir que la normalisation est nécessaire pour éviter qu'une seule variable domine artificiellement le score.

# ===== Cell 074 | code =====
def explode_trends_to_departements(spark: SparkSession, df_trends_clean: "DataFrame") -> "DataFrame":
    """Projette les indices Trends (maille région) vers les départements.

    Crée un DataFrame de mapping région -> liste de départements, puis
    explode pour obtenir une ligne par département. Valide l'exhaustivité
    des 96 départements métropolitains.

    Args:
        spark: Session Spark active.
        df_trends_clean: DataFrame avec region_norm et indice_trends.

    Returns:
        DataFrame avec code_departement et indice_trends (96 lignes).

    Raises:
        ValueError: Si le mapping ne couvre pas tous les départements attendus.
    """
    mapping_schema = StructType([
        StructField("region_norm", StringType(), False),
        StructField("departements", ArrayType(StringType()), False),
    ])
    df_region_deps = spark.createDataFrame(
        list(REGION_TO_DEPARTEMENTS.items()), schema=mapping_schema
    )

    df_region_deps_exploded = df_region_deps.select(
        "region_norm",
        F.explode("departements").alias("code_departement"),
    )

    # Validation exhaustivité
    expected_deps = {f"{i:02d}" for i in range(1, 96) if i != 20} | {"2A", "2B"}
    mapped_deps = {
        r["code_departement"]
        for r in df_region_deps_exploded.select("code_departement").distinct().collect()
    }
    missing = sorted(expected_deps - mapped_deps)
    extra = sorted(mapped_deps - expected_deps)
    if missing or extra:
        raise ValueError(f"Mapping incomplet. manquants={missing}, extras={extra}")

    df_trends_by_departement = (
        df_trends_clean.select("region_norm", "indice_trends")
        .join(df_region_deps_exploded, on="region_norm", how="inner")
    )

    print(f"Regions mappees : {len(REGION_TO_DEPARTEMENTS)}")
    print(f"Departements couverts : {len(mapped_deps)}")
    return df_trends_by_departement

# ===== Cell 075 | markdown =====
# ### 6.2 – Chargement des sources
#
# Cette sous-étape charge toutes les briques déjà préparées pour construire le score final. Elle vérifie que les tables sont cohérentes avant assemblage.
#
# La qualité de cette étape détermine la stabilité de tout le calcul final.

# ===== Cell 076 | code =====
df_trends_by_departement = explode_trends_to_departements(spark, df_trends_clean)
df_trends_by_departement.orderBy('code_departement').show(100, truncate=False)

# ===== Cell 077 | markdown =====
# ### 6.3 – Score concurrentiel
#
# Cette cellule calcule la pression concurrentielle autour de chaque zone. Une zone très saturée reçoit un signal moins favorable.
#
# Illustration réelle observée: score concurrentiel calculé sur 1 809 025 carreaux.

# ===== Cell 078 | code =====
import pyspark.sql.functions as F

OUTPUT_TRENDS = '/home/jovyan/work/data/output/trends_joined/'
df_filo_source = spark.read.parquet(OUTPUT_FILOSOFI)
df_trends_small = df_trends_by_departement.select('code_departement', 'indice_trends')

df_filosofi_trends = (
    df_filo_source
    .join(F.broadcast(df_trends_small), on='code_departement', how='left')
    .withColumnRenamed('indice_trends', 'indice_demande_trends')
    .withColumn(
        'indice_demande_trends',
        F.coalesce(F.col('indice_demande_trends'), F.lit(int(median_indice))),
    )
)
(
    df_filosofi_trends.write
    .mode('overwrite')
    .partitionBy('code_departement')
    .parquet(OUTPUT_TRENDS)
)
print(f'Trends joint sauvegarde dans : {OUTPUT_TRENDS}')
print(f'Carreaux enrichis : {df_filosofi_trends.count()}')

# ===== Cell 079 | markdown =====
# ---
# ## 6 – Score composite d’implantation
#
# C'est l'étape centrale. Les informations de concurrence, accessibilité, immobilier, revenus et trends sont normalisées puis combinées dans un score unique.
#
# Exemple réel observé: 1 797 197 carreaux conservés dans la base finale de scoring.

# ===== Cell 080 | markdown =====
# Cette étape est le cœur du projet. Toutes les informations préparées avant sont regroupées dans un score unique. Le score ne remplace pas le détail, mais il donne une lecture claire pour comparer les zones rapidement. Les tableaux affichés permettent de vérifier que les valeurs restent cohérentes après normalisation et pondération.

# ===== Cell 081 | markdown =====
# ### 6.4 – Assemblage du score final
#
# Cette sous-étape combine toutes les dimensions dans un score unique d'implantation. C'est la synthèse de tout le pipeline.
#
# Illustration réelle observée: base finale de 1 797 197 carreaux après filtre de rentabilité.

# ===== Cell 082 | code =====
# ============================================================

def add_minmax_norm(df: "DataFrame", input_col: str, output_col: str) -> "DataFrame":
    """Normalise une colonne en Min-Max dans [0, 1].

    Robuste au cas dégénéré min == max (retourne 0.5 dans ce cas).
    Déclenche une action Spark (collect) pour récupérer min et max.

    Args:
        df: DataFrame source.
        input_col: Nom de la colonne à normaliser.
        output_col: Nom de la colonne de sortie.

    Returns:
        DataFrame avec la colonne output_col ajoutée.
    """
    stats = df.agg(
        F.min(input_col).alias("min_v"),
        F.max(input_col).alias("max_v"),
    ).collect()[0]

    min_v = float(stats["min_v"] or 0.0)
    max_v = float(stats["max_v"] or 0.0)

    if max_v > min_v:
        return df.withColumn(
            output_col,
            (F.col(input_col) - F.lit(min_v)) / F.lit(max_v - min_v),
        )
    return df.withColumn(output_col, F.lit(0.5))


# Jeux de poids pour l'analyse de sensibilité (nom, conc, access, immo, revenu, demo)
WEIGHT_SETS = [
    ("base",        0.30, 0.25, 0.20, 0.15, 0.10),
    ("conc_plus",   0.40, 0.20, 0.15, 0.15, 0.10),
    ("access_plus", 0.20, 0.35, 0.20, 0.15, 0.10),
    ("immo_plus",   0.25, 0.20, 0.30, 0.15, 0.10),
    ("demo_plus",   0.25, 0.20, 0.15, 0.15, 0.25),
]


# ===== Cell 083 | markdown =====
# ### 6.5 – Export des meilleures zones
#
# Cette étape extrait les zones les plus prometteuses selon le score. Le but est d'obtenir une liste directement exploitable pour l'analyse terrain.
#
# Illustration réelle observée: top 1000 carreaux et 111 communes retenues dans la sortie intermédiaire.

# ===== Cell 084 | code =====
"""
Section 6.1  Initialisation, lecture des sources et utilitaires.
- SparkSession optimisée (local[*], 4g, Arrow).
- Lecture des 4 sources Parquet avec sélection des colonnes utiles.
- Définition des fonctions de normalisation Min-Max robustes (edge min=max -> 0.5).
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from functools import reduce
import pandas as pd

# Initialisation Spark optimisée (contrainte explicite)
spark = SparkSession.builder \
    .appName("PadelSpot_Step6_ScoreFinal") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Poids par défaut (facilement modifiables)
w1 = 0.30  # concurrence
w2 = 0.25  # accessibilité
w3 = 0.20  # immobilier
w4 = 0.15  # revenu
w5 = 0.10  # démographie cible
seuil_rentabilite = 3000.0

# Sources (colonnes utiles uniquement)
df_access = spark.read.parquet("/home/jovyan/work/data/output/accessibilite_clean/").select(
    "IdINSPIRE", "code_commune_insee", "Latitude", "Longitude",
    "part_cible_padel", "score_revenu", "score_accessibilite", "code_departement"
)

df_concurrence = spark.read.parquet("/home/jovyan/work/data/output/concurrence_padel/").select(
    F.col("Nom").alias("nom_club"),
    F.col("Latitude").alias("lat_club"),
    F.col("Longitude").alias("lon_club"),
    F.col("DepartementCode").alias("code_departement")
).filter(F.col("lat_club").isNotNull() & F.col("lon_club").isNotNull())

df_dvf = spark.read.parquet("/home/jovyan/work/data/output/dvf_clean/").select(
    "code_departement", "code_commune", "prix_median_m2"
)

df_trends = spark.read.parquet("/home/jovyan/work/data/output/trends_joined/").select(
    "IdINSPIRE", "indice_demande_trends"
)

def add_minmax_norm(df, input_col: str, output_col: str):
    """Normalisation Min-Max robuste. Si min=max, renvoie 0.5."""
    stats = df.agg(
        F.min(input_col).alias("min_v"),
        F.max(input_col).alias("max_v")
    ).collect()[0]
    min_v = float(stats["min_v"]) if stats["min_v"] is not None else 0.0
    max_v = float(stats["max_v"]) if stats["max_v"] is not None else 0.0
    if max_v > min_v:
        return df.withColumn(output_col, (F.col(input_col) - F.lit(min_v)) / F.lit(max_v - min_v))
    return df.withColumn(output_col, F.lit(0.5))

print(f"ðŸ“¦ accessibilite_clean: {df_access.count():,} lignes")
print(f"ðŸŽ¾ concurrence_padel: {df_concurrence.count():,} lignes")
print(f"ðŸ  dvf_clean: {df_dvf.count():,} lignes")
print(f"ðŸ“ˆ trends_joined: {df_trends.count():,} lignes")

# ===== Cell 085 | markdown =====
# ### 6.6 – Export complet France
#
# Cette sous-étape conserve la couverture nationale complète, pas seulement les meilleures zones. Cela permet des analyses comparatives et des vérifications globales.
#
# Illustration réelle observée: 1 797 197 carreaux scorés dans l'export France complet.

# ===== Cell 086 | code =====
"""
Section 6.2  Score concurrentiel (broadcast + Pandas UDF Haversine vectorisée).
- Broadcast explicite de concurrence_padel (~faible volumétrie relative).
- Jointure par buckets spatiaux + candidats voisins pour éviter un produit cartésien global.
- Calcul vectorisé des distances et agrégation: distance_club_plus_proche, nb_clubs_5km.
- score_concurrence: 1 - norm(nb_clubs_5km) + bonus si distance > 10 km.
"""
@F.pandas_udf(DoubleType())
def haversine_km(lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
    import numpy as np
    r = 6371.0
    lat1_rad = np.radians(lat1.values)
    lon1_rad = np.radians(lon1.values)
    lat2_rad = np.radians(lat2.values)
    lon2_rad = np.radians(lon2.values)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2.0) ** 2
    c = 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))
    return pd.Series(r * c)

# Broadcast obligatoire : dataset de concurrence nettement plus petit que la base carreaux.
# Broadcast Join évite un Shuffle Join global coûteux sur 1.8M+ carreaux.
bucket_deg = 0.05  # ~5.5 km en latitude
neighbor_span = 3  # fenêtre ±3 buckets pour couvrir ~16 km et estimer le plus proche localement

df_clubs_b = df_concurrence \
    .withColumn("lat_b", F.floor(F.col("lat_club") / F.lit(bucket_deg)).cast("int")) \
    .withColumn("lon_b", F.floor(F.col("lon_club") / F.lit(bucket_deg)).cast("int"))

df_cells_b = df_access \
    .withColumn("lat_b", F.floor(F.col("Latitude") / F.lit(bucket_deg)).cast("int")) \
    .withColumn("lon_b", F.floor(F.col("Longitude") / F.lit(bucket_deg)).cast("int"))

neighbors = F.array(*[
    F.struct(
        (F.col("lat_b") + F.lit(dlat)).alias("lat_b_n"),
        (F.col("lon_b") + F.lit(dlon)).alias("lon_b_n")
    )
    for dlat in range(-neighbor_span, neighbor_span + 1)
    for dlon in range(-neighbor_span, neighbor_span + 1)
])

df_candidates = df_cells_b \
    .withColumn("nb", F.explode(neighbors)) \
    .select(
        "IdINSPIRE", "Latitude", "Longitude", "code_commune_insee", "score_accessibilite",
        "score_revenu", "part_cible_padel", "code_departement",
        F.col("nb.lat_b_n").alias("lat_b_n"),
        F.col("nb.lon_b_n").alias("lon_b_n")
    )

df_pairs = df_candidates.join(
    F.broadcast(df_clubs_b),
    (df_candidates["lat_b_n"] == df_clubs_b["lat_b"]) &
    (df_candidates["lon_b_n"] == df_clubs_b["lon_b"]),
    how="left"
)

df_pairs = df_pairs.withColumn(
    "distance_km",
    haversine_km(F.col("Latitude"), F.col("Longitude"), F.col("lat_club"), F.col("lon_club"))
)

df_conc_metrics = df_pairs.groupBy("IdINSPIRE").agg(
    F.min("distance_km").alias("distance_club_plus_proche"),
    F.sum(F.when(F.col("distance_km") <= 5.0, F.lit(1)).otherwise(F.lit(0))).cast("int").alias("nb_clubs_5km")
)

df_step6 = df_access.join(df_conc_metrics, on="IdINSPIRE", how="left") \
    .withColumn("distance_club_plus_proche", F.coalesce(F.col("distance_club_plus_proche"), F.lit(50.0))) \
    .withColumn("nb_clubs_5km", F.coalesce(F.col("nb_clubs_5km"), F.lit(0)))

df_step6 = add_minmax_norm(df_step6, "nb_clubs_5km", "nb_clubs_5km_norm")
df_step6 = df_step6.withColumn("score_concurrence_base", F.lit(1.0) - F.col("nb_clubs_5km_norm"))

# Bonus si le club le plus proche est à plus de 10 km
df_step6 = df_step6.withColumn(
    "score_concurrence",
    F.least(
        F.lit(1.0),
        F.col("score_concurrence_base") + F.when(F.col("distance_club_plus_proche") > 10.0, F.lit(0.10)).otherwise(F.lit(0.0))
    )
).drop("score_concurrence_base", "nb_clubs_5km_norm")

print(f" Score concurrentiel calculé : {df_step6.count():,} carreaux")
df_step6.select("IdINSPIRE", "distance_club_plus_proche", "nb_clubs_5km", "score_concurrence").show(10, truncate=False)

# ===== Cell 087 | markdown =====
# ### 6.4 – Assemblage du score final
#
# On joint tous les scores partiels et on calcule le score composite pour chaque jeu de poids. Le score `base` est retenu comme référence.

# ===== Cell 088 | code =====
"""
Section 6.3  Score immobilier, demande Trends, score composite et stabilité.
- Jointure DVF en left join avec fallback médiane nationale.
- Exclusion des carreaux non rentables (prix_median_m2 > 3000 â‚¬/mÂ²).
- Jointure left Trends + normalisation robuste.
- Calcul score_final + analyse de sensibilité (5 combinaisons de poids).
"""
# Repartir d'une base propre de colonnes Step6 (évite les duplications en cas de rerun)
df_step6 = df_step6.select(
    "IdINSPIRE", "code_commune_insee", "Latitude", "Longitude",
    "part_cible_padel", "score_revenu", "score_accessibilite", "code_departement",
    "distance_club_plus_proche", "nb_clubs_5km", "score_concurrence"
)

# -------------------------
# Score Immobilier
# -------------------------
# Extraction d'un code commune compatible DVF (3 derniers chars du premier code INSEE trouvé).
# Exemples: 24534 -> 534, 2A041 -> 041
df_step6 = df_step6.withColumn(
    "code_commune_join",
    F.regexp_extract(F.upper(F.col("code_commune_insee")), r"([0-9A-B]{5})", 1)
).withColumn(
    "code_commune_3",
    F.when(F.length(F.col("code_commune_join")) == 5, F.substring(F.col("code_commune_join"), 3, 3)).otherwise(F.lit(None))
)

dvf_median_row = df_dvf.agg(F.expr("percentile_approx(prix_median_m2, 0.5, 1000)").alias("median_dvf")).collect()[0]
median_prix_m2 = float(dvf_median_row["median_dvf"]) if dvf_median_row["median_dvf"] is not None else 1500.0

df_dvf_join = df_dvf.select(
    F.col("code_departement").alias("dvf_code_departement"),
    F.col("code_commune").alias("dvf_code_commune"),
    F.col("prix_median_m2").alias("dvf_prix_median_m2")
)

df_step6 = df_step6.join(
    df_dvf_join,
    (df_step6["code_departement"] == df_dvf_join["dvf_code_departement"]) &
    (df_step6["code_commune_3"] == df_dvf_join["dvf_code_commune"]),
    how="left"
)

df_step6 = df_step6.withColumn("prix_median_m2", F.coalesce(F.col("dvf_prix_median_m2"), F.lit(median_prix_m2))) \
    .drop("dvf_code_departement", "dvf_code_commune", "dvf_prix_median_m2")

df_step6 = add_minmax_norm(df_step6, "prix_median_m2", "prix_m2_norm")
df_step6 = df_step6.withColumn("score_immobilier", F.lit(1.0) - F.col("prix_m2_norm"))

# Exclusion non rentable
df_step6 = df_step6.filter(F.col("prix_median_m2") <= F.lit(seuil_rentabilite))

# -------------------------
# Trends
# -------------------------
trend_median_row = df_trends.agg(F.expr("percentile_approx(indice_demande_trends, 0.5, 1000)").alias("median_trends")).collect()[0]
median_trends = int(trend_median_row["median_trends"]) if trend_median_row["median_trends"] is not None else 50

df_trends_join = df_trends.select(
    F.col("IdINSPIRE").alias("trend_IdINSPIRE"),
    F.col("indice_demande_trends").alias("trend_indice_demande_trends")
)

df_step6 = df_step6.join(
    df_trends_join,
    df_step6["IdINSPIRE"] == df_trends_join["trend_IdINSPIRE"],
    how="left"
).drop("trend_IdINSPIRE")

df_step6 = df_step6.withColumn(
    "indice_demande_trends",
    F.coalesce(F.col("trend_indice_demande_trends"), F.lit(median_trends))
).drop("trend_indice_demande_trends")

df_step6 = add_minmax_norm(df_step6, "indice_demande_trends", "indice_demande_trends_norm")

# -------------------------
# Score final (pondérations par défaut)
# -------------------------
score_core = (
    F.lit(w1) * F.col("score_concurrence") +
    F.lit(w2) * F.col("score_accessibilite") +
    F.lit(w3) * F.col("score_immobilier") +
    F.lit(w4) * F.col("score_revenu") +
    F.lit(w5) * F.col("part_cible_padel")
)

df_step6 = df_step6.withColumn("score_final", score_core * F.col("indice_demande_trends_norm"))

# -------------------------
# Analyse de sensibilité (5 combinaisons)
# -------------------------
weight_sets = [
    ("base", 0.30, 0.25, 0.20, 0.15, 0.10),
    ("conc_plus", 0.40, 0.20, 0.15, 0.15, 0.10),
    ("access_plus", 0.20, 0.35, 0.20, 0.15, 0.10),
    ("immo_plus", 0.25, 0.20, 0.30, 0.15, 0.10),
    ("demo_plus", 0.25, 0.20, 0.15, 0.15, 0.25),
]

tops = []
for combo_name, a, b, c, d, e in weight_sets:
    variant_score = (
        F.lit(a) * F.col("score_concurrence") +
        F.lit(b) * F.col("score_accessibilite") +
        F.lit(c) * F.col("score_immobilier") +
        F.lit(d) * F.col("score_revenu") +
        F.lit(e) * F.col("part_cible_padel")
    ) * F.col("indice_demande_trends_norm")

    df_variant = df_step6.select("IdINSPIRE", variant_score.alias("score_variant"))
    w_top = Window.orderBy(F.desc("score_variant"))
    df_top500 = df_variant.withColumn("rn", F.row_number().over(w_top)) \
        .filter(F.col("rn") <= 500) \
        .select("IdINSPIRE") \
        .withColumn("combo", F.lit(combo_name))
    tops.append(df_top500)

df_tops_union = reduce(lambda x, y: x.unionByName(y), tops)
df_stabilite = df_tops_union.groupBy("IdINSPIRE").agg(F.countDistinct("combo").alias("stabilite_score"))

df_step6 = df_step6.join(df_stabilite, on="IdINSPIRE", how="left") \
    .withColumn("stabilite_score", F.coalesce(F.col("stabilite_score"), F.lit(0)))

print(f" Base Step6 prÃªte : {df_step6.count():,} carreaux aprÃ¨s filtre rentabilité")
df_step6.select("IdINSPIRE", "score_concurrence", "score_accessibilite", "score_immobilier", "score_final", "stabilite_score").show(10, truncate=False)

# ===== Cell 089 | markdown =====
# ### 6.5 – Export des meilleures zones
#
# On isole le top des carreaux selon le score composite. L'export GeoJSON permet de visualiser les zones dans QGIS ou Folium.

# ===== Cell 090 | code =====
"""
Section 6.4  Top zones, export Parquet et GeoJSON (Folium compatible).
- Top 1000 carreaux par score_final.
- Agrégation communale (score moyen, nb carreaux, centroïde).
- Filtre communes avec >= 3 carreaux (robustesse).
- Export parquet partitionné + GeoJSON FeatureCollection WGS84.
"""
import json
import os

# Top 1000 carreaux
w_rank = Window.orderBy(F.desc("score_final"))
df_top1000 = df_step6.withColumn("rank_final", F.row_number().over(w_rank)) \
    .filter(F.col("rank_final") <= 1000)

# Agrégation par commune
df_top_communes = df_top1000.groupBy("code_commune_insee", "code_departement").agg(
    F.count("*").alias("nb_carreaux_top1000"),
    F.avg("score_final").alias("score_final_moyen"),
    F.avg("score_concurrence").alias("score_concurrence_moyen"),
    F.avg("score_accessibilite").alias("score_accessibilite_moyen"),
    F.avg("score_immobilier").alias("score_immobilier_moyen"),
    F.avg("score_revenu").alias("score_revenu_moyen"),
    F.avg("part_cible_padel").alias("part_cible_padel_moyen"),
    F.avg(F.col("indice_demande_trends").cast("double")).alias("indice_demande_trends_moyen"),
    F.avg(F.col("stabilite_score").cast("double")).alias("stabilite_score_moyen"),
    F.avg("Latitude").alias("latitude_centroid"),
    F.avg("Longitude").alias("longitude_centroid")
).filter(F.col("nb_carreaux_top1000") >= 3)

# Export Parquet final (carreaux)
output_score_final = "/home/jovyan/work/data/output/score_final/"
df_top1000.write \
    .mode("overwrite") \
    .partitionBy("code_departement") \
    .parquet(output_score_final)

# Export GeoJSON (communes) compatible Folium
geojson_path = "/home/jovyan/work/data/output/top_zones.geojson"
os.makedirs(os.path.dirname(geojson_path), exist_ok=True)

rows = df_top_communes.collect()
features = []
for r in rows:
    lon = float(r["longitude_centroid"]) if r["longitude_centroid"] is not None else None
    lat = float(r["latitude_centroid"]) if r["latitude_centroid"] is not None else None
    if lon is None or lat is None:
        continue

    properties = {
        "code_commune_insee": r["code_commune_insee"],
        "code_departement": r["code_departement"],
        "nb_carreaux_top1000": int(r["nb_carreaux_top1000"]),
        "score_final_moyen": float(r["score_final_moyen"]),
        "score_concurrence_moyen": float(r["score_concurrence_moyen"]),
        "score_accessibilite_moyen": float(r["score_accessibilite_moyen"]),
        "score_immobilier_moyen": float(r["score_immobilier_moyen"]),
        "score_revenu_moyen": float(r["score_revenu_moyen"]),
        "part_cible_padel_moyen": float(r["part_cible_padel_moyen"]),
        "indice_demande_trends_moyen": float(r["indice_demande_trends_moyen"]),
        "stabilite_score_moyen": float(r["stabilite_score_moyen"]),
    }

    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [lon, lat],
        },
        "properties": properties,
    }
    features.append(feature)

geojson = {
    "type": "FeatureCollection",
    "features": features,
}

with open(geojson_path, "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False)

print(f" Parquet score final sauvegardé : {output_score_final}")
print(f" GeoJSON top zones sauvegardé : {geojson_path}")
print(f"ðŸ“Š Top1000 carreaux: {df_top1000.count():,}")
print(f"ðŸ—ºï¸ Communes retenues (>=3 carreaux top1000): {len(features):,}")
df_top_communes.orderBy(F.desc("score_final_moyen")).show(20, truncate=False)

# ===== Cell 091 | markdown =====
# ### 7.1 – Schéma de sortie : apply_dash_schema()
#
# Cette fonction impose un format de colonnes stable pour toutes les tables finales. Elle gère les types et les valeurs manquantes de manière cohérente.
#
# Dans un cadre académique, cette étape est essentielle car elle rend les résultats comparables et réutilisables dans d'autres outils.

# ===== Cell 092 | code =====
"""
Section 6.5  Export complet France (tous les carreaux scorés).
- Conserve l'ensemble de df_step6 pour cartographie nationale.
- Export parquet partitionné par code_departement.
"""
output_score_final_full = "/home/jovyan/work/data/output/score_final_full/"

df_step6.write \
    .mode("overwrite") \
    .partitionBy("code_departement") \
    .parquet(output_score_final_full)

print(f" Export complet score final : {output_score_final_full}")
print(f"ðŸ“Š Nombre total de carreaux scorés (France) : {df_step6.count():,}")
print("ðŸ“¦ Nombre de départements présents :")
df_step6.select("code_departement").distinct().count()

# ===== Cell 093 | markdown =====
# ---
# ## 7 – Préparation des exports Dash Ready
#
# Cette étape transforme les résultats analytiques en tables prêtes pour une application de visualisation.
#
# Exemple réel observé: 122 236 lignes dans dash_communes_agg et 18 637 lignes dans dash_clubs.

# ===== Cell 094 | markdown =====
# Ici, la logique est de passer d’une table analytique à des tables prêtes pour une application. Les schémas sont stabilisés, les champs sont homogènes et les fichiers de sortie deviennent faciles à réutiliser. Cette étape rend le travail exploitable au-delà du notebook.

# ===== Cell 095 | markdown =====
# ### 7.1 – Schéma de sortie : `apply_dash_schema()`
#
# Cette fonction standardise les types et remplace les nulls par des valeurs par défaut cohérentes (0.0 pour les scores, -1 pour les entiers, 'N/A' pour les strings). On utilise un `fillna` groupé et un `select()` vectorisé pour éviter de fragmenter le DAG Spark.

# ===== Cell 096 | code =====
# ============================================================

def _first_existing_path(candidates: list[str]) -> "str | None":
    """Retourne le premier chemin existant dans la liste, ou None.

    Args:
        candidates: Liste de chemins à tester dans l'ordre.

    Returns:
        Premier chemin existant, ou None si aucun ne l'est.
    """
    import os
    return next((p for p in candidates if os.path.exists(p)), None)


def _bytes_to_mb(value: float) -> float:
    """Convertit des octets en mégaoctets arrondis à 2 décimales.

    Args:
        value: Taille en octets.

    Returns:
        Taille en Mo (float, 2 décimales).
    """
    return round(float(value) / (1024.0 * 1024.0), 2)


def _path_size_bytes(path: str) -> int:
    """Calcule la taille totale d'un fichier ou d'un répertoire en octets.

    Args:
        path: Chemin vers un fichier ou un répertoire.

    Returns:
        Taille totale en octets (0 si le chemin n'existe pas).
    """
    import os
    if not os.path.exists(path):
        return 0
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _, files in os.walk(path):
        for fname in files:
            fp = os.path.join(root, fname)
            if os.path.isfile(fp):
                total += os.path.getsize(fp)
    return total


def apply_dash_schema(df_dash: "DataFrame") -> "DataFrame":
    """Applique la nomenclature finale, le remplissage des nulls et les casts de types.

    Remplace les boucles Python withColumn par :
    - fillna() groupé (1 seule opération Spark)
    - select() vectorisé pour les casts de types

    Args:
        df_dash: DataFrame brut avant export Dash.

    Returns:
        DataFrame avec types optimisés et nulls remplis.
    """
    # Renommage snake_case via withColumnRenamed (minimal, colonnes PascalCase héritées)
    rename_map = {
        "IdINSPIRE": "id_inspire",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "Men_pauv": "men_pauv",
        "Ind_18_24": "ind_18_24",
        "Ind_25_39": "ind_25_39",
        "I_pauv": "i_pauv",
        "Ind": "ind",
        "Ind_snv": "ind_snv",
    }
    for old_col, new_col in rename_map.items():
        if old_col in df_dash.columns:
            df_dash = df_dash.withColumnRenamed(old_col, new_col)

    # fillna groupé (1 opération au lieu de N withColumn)
    score_cols = [
        "score_final", "score_concurrence", "score_accessibilite",
        "score_immobilier", "score_revenu", "part_cible_padel",
        "indice_demande_trends", "distance_club_plus_proche",
        "prix_median_m2", "ind", "ind_snv", "men_pauv",
        "ind_18_24", "ind_25_39", "i_pauv", "densite_tc", "proximite_axe",
    ]
    int_cols = ["stabilite_score", "nb_clubs_5km", "nb_transactions", "population_estimee"]
    str_cols = ["id_inspire", "code_commune_insee", "code_departement", "note_lettre", "concurrence_label"]

    double_defaults = {c: 0.0 for c in score_cols if c in df_dash.columns}
    int_defaults = {c: -1 for c in int_cols if c in df_dash.columns}
    str_defaults = {c: "N/A" for c in str_cols if c in df_dash.columns}
    df_dash = df_dash.fillna({**double_defaults, **int_defaults, **str_defaults})

    # Casts de types via select() vectorisé (1 plan logique)
    float32_cols = [
        "score_final", "score_concurrence", "score_accessibilite", "score_immobilier",
        "score_revenu", "part_cible_padel", "indice_demande_trends",
        "distance_club_plus_proche", "prix_median_m2", "ind", "ind_snv",
        "men_pauv", "ind_18_24", "ind_25_39", "i_pauv", "densite_tc", "proximite_axe",
    ]
    int16_cols = ["stabilite_score", "nb_clubs_5km", "nb_transactions"]

    df_dash = df_dash.select(
        *[
            F.col(c).cast(FloatType()).alias(c)
            if c in float32_cols and c in df_dash.columns
            else F.col(c).cast(ShortType()).alias(c)
            if c in int16_cols and c in df_dash.columns
            else F.col(c)
            for c in df_dash.columns
        ]
    )

    if "population_estimee" in df_dash.columns:
        df_dash = df_dash.withColumn(
            "population_estimee", F.col("population_estimee").cast(IntegerType())
        )

    return df_dash

# ===== Cell 097 | markdown =====
# ### 7.2 – Construction de la table principale
#
# Cette cellule construit la table centrale Dash Ready à partir du score final. Elle sert de base commune pour toutes les vues et agrégations.
#
# La lecture de sortie doit vérifier le volume écrit et la cohérence des colonnes clés.

# ===== Cell 098 | code =====
"""
ETAPE 7 - CELLULE 1/3
Preparation de la table principale dash_carreaux_full (grain carreau INSEE) via jointures Spark.
- Lecture des sources avec schemas explicites pour les CSV.
- Jointures lourdes uniquement dans Spark.
- Optimisation des types (float32/int16) pour reduire le volume disque.
- Export Parquet partitionne par code_departement + verification de comptage.
"""

import os
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, FloatType, ShortType, IntegerType

PATHS = {
    "accessibilite_clean": "/home/jovyan/work/data/output/accessibilite_clean/",
    "filosofi_clean": "/home/jovyan/work/data/output/filosofi_clean/",
    "dvf_clean": "/home/jovyan/work/data/output/dvf_clean/",
    "concurrence_padel": "/home/jovyan/work/data/output/clubs_concurrents/concurrence_padel/",
    "clubs_concurrents": "/home/jovyan/work/data/output/clubs_concurrents/",
    "trends_joined": "/home/jovyan/work/data/output/trends_joined/",
    "score_final": "/home/jovyan/work/data/output/score_final/",
    "score_final_full": "/home/jovyan/work/data/output/score_final_full/",
    "osm_transport": "/home/jovyan/work/data/output/osm_transport_stops.csv",
    "osm_roads": "/home/jovyan/work/data/output/osm_road_axes.csv",
    "top_zones_geojson": "/home/jovyan/work/data/output/top_zones.geojson",
}
OUTPUT_DASH = "/home/jovyan/work/data/dash_ready/"
os.makedirs(OUTPUT_DASH, exist_ok=True)

# Spark session dediee ETAPE 7
spark = SparkSession.builder \
    .appName("PadelSpot_Step7_DashReady") \
    .master("local[*]") \
    .config("spark.driver.memory", "6g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Helpers

def _first_existing_path(candidates):
    return next((p for p in candidates if os.path.exists(p)), None)


def _bytes_to_mb(value):
    return round(float(value) / (1024.0 * 1024.0), 2)


# Lecture des sources principales
df_score_full = spark.read.parquet(PATHS["score_final_full"]).select(
    "IdINSPIRE", "code_commune_insee", "code_departement",
    "Latitude", "Longitude",
    "score_final", "score_concurrence", "score_accessibilite",
    "score_immobilier", "score_revenu", "stabilite_score",
    "part_cible_padel", "indice_demande_trends",
    "distance_club_plus_proche", "nb_clubs_5km",
    "prix_median_m2"
)

df_trends = spark.read.parquet(PATHS["trends_joined"]).select(
    F.col("IdINSPIRE").alias("tr_IdINSPIRE"),
    F.col("indice_demande_trends").alias("tr_indice_demande_trends")
)

df_access = spark.read.parquet(PATHS["accessibilite_clean"]).select(
    F.col("IdINSPIRE").alias("ac_IdINSPIRE"),
    "densite_tc", "proximite_axe"
)

df_filo = spark.read.parquet(PATHS["filosofi_clean"]).select(
    F.col("IdINSPIRE").alias("fi_IdINSPIRE"),
    "Men_pauv", "Ind_18_24", "Ind_25_39", "I_pauv", "Ind", "Ind_snv"
)

df_dvf = spark.read.parquet(PATHS["dvf_clean"]).select(
    "code_departement", "code_commune", "prix_median_m2", "nb_transactions"
)

# Jointures heavy data -> Spark uniquement
# 1) score_full + trends
df_dash = df_score_full.join(
    df_trends,
    df_score_full["IdINSPIRE"] == df_trends["tr_IdINSPIRE"],
    how="left"
).drop("tr_IdINSPIRE")

# 2) + accessibilite
df_dash = df_dash.join(
    df_access,
    df_dash["IdINSPIRE"] == df_access["ac_IdINSPIRE"],
    how="left"
).drop("ac_IdINSPIRE")

# 3) + filosofi
df_dash = df_dash.join(
    df_filo,
    df_dash["IdINSPIRE"] == df_filo["fi_IdINSPIRE"],
    how="left"
).drop("fi_IdINSPIRE")

# 4) + DVF (broadcast). Join robuste sur code_departement + code_commune extrait.
df_dash = df_dash.withColumn(
    "code_commune_3",
    F.when(
        F.length(F.regexp_extract(F.upper(F.col("code_commune_insee")), r"([0-9A-B]{5})", 1)) == 5,
        F.substring(F.regexp_extract(F.upper(F.col("code_commune_insee")), r"([0-9A-B]{5})", 1), 3, 3)
    ).otherwise(F.col("code_commune_insee"))
)

df_dvf_b = F.broadcast(
    df_dvf.select(
        F.col("code_departement").alias("dvf_code_departement"),
        F.col("code_commune").alias("dvf_code_commune"),
        F.col("prix_median_m2").alias("dvf_prix_median_m2"),
        F.col("nb_transactions").alias("dvf_nb_transactions")
    )
)

df_dash = df_dash.join(
    df_dvf_b,
    (df_dash["code_departement"] == df_dvf_b["dvf_code_departement"]) &
    (df_dash["code_commune_3"] == df_dvf_b["dvf_code_commune"]),
    how="left"
)

df_dash = df_dash.withColumn(
    "prix_median_m2",
    F.coalesce(F.col("prix_median_m2"), F.col("dvf_prix_median_m2"))
).withColumn(
    "nb_transactions",
    F.col("dvf_nb_transactions")
).drop("dvf_code_departement", "dvf_code_commune", "dvf_prix_median_m2", "dvf_nb_transactions", "code_commune_3")

# Colonnes calculees
# note_lettre: A+ >0.8, A >0.7, B >0.6, C >0.5, D sinon
df_dash = df_dash.withColumn(
    "note_lettre",
    F.when(F.col("score_final") > 0.8, F.lit("A+"))
     .when(F.col("score_final") > 0.7, F.lit("A"))
     .when(F.col("score_final") > 0.6, F.lit("B"))
     .when(F.col("score_final") > 0.5, F.lit("C"))
     .otherwise(F.lit("D"))
)

# concurrence_label
df_dash = df_dash.withColumn(
    "concurrence_label",
    F.when(F.col("nb_clubs_5km") == 0, F.lit("Zone blanche"))
     .when(F.col("nb_clubs_5km") <= 2, F.lit("Faible"))
     .when(F.col("nb_clubs_5km") <= 5, F.lit("Concurrentiel"))
     .otherwise(F.lit("Sature"))
)

# population_estimee = ind * 25
df_dash = df_dash.withColumn(
    "population_estimee",
    F.round(F.col("Ind") * F.lit(25.0)).cast(IntegerType())
)

# Renommage snake_case final
rename_map = {
    "IdINSPIRE": "id_inspire",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Men_pauv": "men_pauv",
    "Ind_18_24": "ind_18_24",
    "Ind_25_39": "ind_25_39",
    "I_pauv": "i_pauv",
    "Ind": "ind",
    "Ind_snv": "ind_snv",
}

for old_col, new_col in rename_map.items():
    if old_col in df_dash.columns:
        df_dash = df_dash.withColumnRenamed(old_col, new_col)

# Fillna systematique
score_cols = [
    "score_final", "score_concurrence", "score_accessibilite",
    "score_immobilier", "score_revenu", "part_cible_padel",
    "indice_demande_trends", "distance_club_plus_proche",
    "prix_median_m2", "ind", "ind_snv", "men_pauv",
    "ind_18_24", "ind_25_39", "i_pauv", "densite_tc", "proximite_axe"
]

for c in score_cols:
    if c in df_dash.columns:
        df_dash = df_dash.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))

int_unknown_cols = ["stabilite_score", "nb_clubs_5km", "nb_transactions", "population_estimee"]
for c in int_unknown_cols:
    if c in df_dash.columns:
        df_dash = df_dash.withColumn(c, F.coalesce(F.col(c), F.lit(-1)))

string_cols = ["id_inspire", "code_commune_insee", "code_departement", "note_lettre", "concurrence_label"]
for c in string_cols:
    if c in df_dash.columns:
        df_dash = df_dash.withColumn(c, F.coalesce(F.col(c).cast(StringType()), F.lit("N/A")))

# Optimisation des types: float32 pour mesures (sauf lat/lon en float64)
float32_cols = [
    "score_final", "score_concurrence", "score_accessibilite", "score_immobilier",
    "score_revenu", "part_cible_padel", "indice_demande_trends",
    "distance_club_plus_proche", "prix_median_m2", "ind", "ind_snv",
    "men_pauv", "ind_18_24", "ind_25_39", "i_pauv",
    "densite_tc", "proximite_axe"
]
for c in float32_cols:
    if c in df_dash.columns:
        df_dash = df_dash.withColumn(c, F.col(c).cast(FloatType()))

# Entiers en int16 quand possible
int16_cols = ["stabilite_score", "nb_clubs_5km", "nb_transactions"]
for c in int16_cols:
    if c in df_dash.columns:
        df_dash = df_dash.withColumn(c, F.col(c).cast(ShortType()))

# population_estimee en int32 (securite volume)
if "population_estimee" in df_dash.columns:
    df_dash = df_dash.withColumn("population_estimee", F.col("population_estimee").cast(IntegerType()))

# Selection ordonnee finale
final_cols = [
    "id_inspire", "code_commune_insee", "code_departement",
    "latitude", "longitude",
    "score_final", "score_concurrence", "score_accessibilite",
    "score_immobilier", "score_revenu", "stabilite_score",
    "part_cible_padel", "indice_demande_trends",
    "distance_club_plus_proche", "nb_clubs_5km",
    "prix_median_m2", "nb_transactions",
    "ind", "ind_snv", "men_pauv", "ind_18_24", "ind_25_39", "i_pauv",
    "densite_tc", "proximite_axe",
    "note_lettre", "concurrence_label", "population_estimee"
]

df_dash_carreaux_full = df_dash.select(*[c for c in final_cols if c in df_dash.columns])

# Export
path_dash_carreaux_full = os.path.join(OUTPUT_DASH, "dash_carreaux_full")
df_dash_carreaux_full.write \
    .mode("overwrite") \
    .partitionBy("code_departement") \
    .parquet(path_dash_carreaux_full)

count_dash_carreaux_full = spark.read.parquet(path_dash_carreaux_full).count()
print(f"[1/7] dash_carreaux_full : {count_dash_carreaux_full:,} lignes")
print(f" Verification write parquet OK -> {path_dash_carreaux_full}")

# ===== Cell 099 | markdown =====
# ### 7.3 – Tables dérivées (communes, clubs, transport, départements)
#
# Ici, la table principale est déclinée en tables spécialisées pour faciliter l'usage applicatif. Chaque table répond à un besoin précis de lecture.
#
# Illustration réelle observée: dash_communes_agg contient 122 236 lignes et dash_clubs contient 18 637 lignes.

# ===== Cell 100 | code =====
"""
ETAPE 7 - CELLULE 2/3
Construction des tables derivees Dash-ready:
- dash_communes_agg
- dash_top_zones (parquet + geojson)
- dash_clubs (parquet + csv)
- dash_transport
- dash_roads
- dash_departements_stats (parquet + csv)
"""

import os
import json
import geopandas as gpd
import pandas as pd
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, BooleanType

path_dash_carreaux_full = os.path.join(OUTPUT_DASH, "dash_carreaux_full")
df_dash_carreaux_full = spark.read.parquet(path_dash_carreaux_full)

# ============================================================
# [2/7] dash_communes_agg
# ============================================================
path_dash_communes_agg = os.path.join(OUTPUT_DASH, "dash_communes_agg")

df_dash_communes_agg = df_dash_carreaux_full.groupBy("code_commune_insee").agg(
    F.first("code_departement", ignorenulls=True).alias("code_departement"),
    F.count("*").alias("nb_carreaux"),
    F.avg("score_final").alias("score_final_moyen"),
    F.max("score_final").alias("score_final_max"),
    F.avg("score_concurrence").alias("score_concurrence_moyen"),
    F.avg("score_accessibilite").alias("score_accessibilite_moyen"),
    F.avg("score_immobilier").alias("score_immobilier_moyen"),
    F.avg("score_revenu").alias("score_revenu_moyen"),
    F.avg("part_cible_padel").alias("part_cible_padel_moyen"),
    F.avg("indice_demande_trends").alias("indice_demande_trends_moyen"),
    F.avg("stabilite_score").alias("stabilite_score_moyen"),
    F.avg("latitude").alias("lat_centre"),
    F.avg("longitude").alias("lon_centre")
)

# Remplacement des boucles withColumn par un unique fillna (optimisation)
col_zeros = [
    "score_final_moyen", "score_final_max", "score_concurrence_moyen", "score_accessibilite_moyen",
    "score_immobilier_moyen", "score_revenu_moyen", "part_cible_padel_moyen",
    "indice_demande_trends_moyen", "stabilite_score_moyen"
]
df_dash_communes_agg = df_dash_communes_agg.fillna(
    {**{c: 0.0 for c in col_zeros}, "code_departement": "N/A", "code_commune_insee": "N/A", "nb_carreaux": -1}
)

df_dash_communes_agg.write.mode("overwrite").parquet(path_dash_communes_agg)
count_dash_communes_agg = spark.read.parquet(path_dash_communes_agg).count()
print(f"[2/7] dash_communes_agg : {count_dash_communes_agg:,} lignes")
print(f"Verification write parquet OK -> {path_dash_communes_agg}")


# ============================================================
# [3/7] dash_top_zones (parquet + geojson)
# ============================================================
path_dash_top_zones_parquet = os.path.join(OUTPUT_DASH, "dash_top_zones.parquet")
path_dash_top_zones_geojson = os.path.join(OUTPUT_DASH, "dash_top_zones.geojson")

# Base Spark (top 1000)
df_top1000 = spark.read.parquet(PATHS["score_final"]).select(
    "IdINSPIRE", "code_commune_insee", "code_departement", "Latitude", "Longitude", "score_final"
)

w_nat = Window.orderBy(F.desc("score_final"))
w_dep = Window.partitionBy("code_departement").orderBy(F.desc("score_final"))

df_top1000 = df_top1000.withColumn("rang_national", F.row_number().over(w_nat)) \
    .withColumn("rang_departement", F.row_number().over(w_dep)) \
    .withColumn(
        "tier",
        F.when(F.col("score_final") > 0.75, F.lit("Premium"))
         .when(F.col("score_final") > 0.65, F.lit("Excellent"))
         .when(F.col("score_final") > 0.55, F.lit("Bon"))
         .otherwise(F.lit("Correct"))
    ) \
    .withColumn(
        "label_carte",
        F.concat(
            F.col("code_commune_insee"),
            F.lit(" - "),
            F.format_string("%.0f%%", F.col("score_final") * 100.0)
        )
    )

# Merge avec GeoJSON (petit volume => geopandas autorise)
if os.path.exists(PATHS["top_zones_geojson"]):
    gdf_top = gpd.read_file(PATHS["top_zones_geojson"])
else:
    gdf_top = gpd.GeoDataFrame(columns=["code_commune_insee", "geometry"], geometry="geometry", crs="EPSG:4326")

top_pdf = df_top1000.toPandas()
if "code_commune_insee" not in gdf_top.columns:
    gdf_top["code_commune_insee"] = None

merged_pdf = top_pdf.merge(
    gdf_top[[c for c in ["code_commune_insee", "geometry"] if c in gdf_top.columns]],
    on="code_commune_insee",
    how="left"
)

# GeoDataFrame final pour export geojson
geo_cols = [
    "IdINSPIRE", "code_commune_insee", "code_departement", "Latitude", "Longitude", "score_final",
    "rang_national", "rang_departement", "tier", "label_carte"
]

gdf_dash_top = gpd.GeoDataFrame(merged_pdf, geometry="geometry", crs="EPSG:4326")
if gdf_dash_top.geometry.isna().any():
    gdf_dash_top["geometry"] = gdf_dash_top["geometry"].where(
        ~gdf_dash_top["geometry"].isna(),
        gpd.points_from_xy(gdf_dash_top["Longitude"], gdf_dash_top["Latitude"], crs="EPSG:4326")
    )

gdf_dash_top.to_file(path_dash_top_zones_geojson, driver="GeoJSON")

# Parquet sans geometrie
spark.createDataFrame(merged_pdf[geo_cols]).write.mode("overwrite").parquet(path_dash_top_zones_parquet)
count_dash_top = spark.read.parquet(path_dash_top_zones_parquet).count()
print(f"[3/7] dash_top_zones : {count_dash_top:,} lignes")
print(f"Verification write parquet OK -> {path_dash_top_zones_parquet}")


# ============================================================
# [4/7] dash_clubs
# ============================================================
path_dash_clubs_parquet = os.path.join(OUTPUT_DASH, "dash_clubs.parquet")
path_dash_clubs_csv = os.path.join(OUTPUT_DASH, "dash_clubs.csv")

conc_path = _first_existing_path([
    PATHS["concurrence_padel"],
    "/home/jovyan/work/data/output/concurrence_padel/"
])
if conc_path is None:
    raise FileNotFoundError("Aucun chemin concurrence_padel valide trouve.")

annexe_path = _first_existing_path([
    PATHS["clubs_concurrents"],
    "/home/jovyan/work/data/output/clubs_concurrents/"
])
if annexe_path is None:
    raise FileNotFoundError("Aucun chemin clubs_concurrents valide trouve.")

df_conc_main = spark.read.parquet(conc_path).select(
    F.coalesce(F.col("Nom"), F.lit("N/A")).alias("nom"),
    F.coalesce(F.col("Type"), F.lit("N/A")).alias("type"),
    F.coalesce(F.col("Commune"), F.lit("N/A")).alias("commune"),
    F.col("Latitude").cast(DoubleType()).alias("latitude"),
    F.col("Longitude").cast(DoubleType()).alias("longitude"),
    F.col("Nombre_de_courts").cast(IntegerType()).alias("nombre_de_courts"),
    F.coalesce(F.col("Source"), F.lit("N/A")).alias("source"),
    F.coalesce(F.col("DepartementCode"), F.lit("N/A")).alias("departement_code")
).withColumn("source_principale", F.lit(True))

# Table annexe potentiellement heterogene
try:
    df_conc_annexe_raw = spark.read.parquet(annexe_path)
except Exception:
    df_conc_annexe_raw = spark.createDataFrame([], df_conc_main.schema)

for needed, fallback in [
    ("Nom", "nom"), ("Type", "type"), ("Commune", "commune"),
    ("Latitude", "latitude"), ("Longitude", "longitude"),
    ("Nombre_de_courts", "nombre_de_courts"), ("Source", "source"),
    ("DepartementCode", "departement_code"),
]:
    if needed not in df_conc_annexe_raw.columns:
        if fallback in df_conc_annexe_raw.columns:
            df_conc_annexe_raw = df_conc_annexe_raw.withColumnRenamed(fallback, needed)
        else:
            df_conc_annexe_raw = df_conc_annexe_raw.withColumn(needed, F.lit(None))

df_conc_annexe = df_conc_annexe_raw.select(
    F.coalesce(F.col("Nom"), F.lit("N/A")).alias("nom"),
    F.coalesce(F.col("Type"), F.lit("N/A")).alias("type"),
    F.coalesce(F.col("Commune"), F.lit("N/A")).alias("commune"),
    F.col("Latitude").cast(DoubleType()).alias("latitude"),
    F.col("Longitude").cast(DoubleType()).alias("longitude"),
    F.col("Nombre_de_courts").cast(IntegerType()).alias("nombre_de_courts"),
    F.coalesce(F.col("Source"), F.lit("N/A")).alias("source"),
    F.coalesce(F.col("DepartementCode"), F.lit("N/A")).alias("departement_code")
).withColumn("source_principale", F.lit(False))

df_clubs_union = df_conc_main.unionByName(df_conc_annexe, allowMissingColumns=True) \
    .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())

# Dedup GPS arrondi 3 decimales avec priorite a concurrence_padel
df_clubs_union = df_clubs_union.withColumn("lat3", F.round("latitude", 3)) \
    .withColumn("lon3", F.round("longitude", 3))

w_dedup = Window.partitionBy("lat3", "lon3").orderBy(F.desc("source_principale"), F.col("nombre_de_courts").desc_nulls_last())
df_clubs_dedup = df_clubs_union.withColumn("rn", F.row_number().over(w_dedup)) \
    .filter(F.col("rn") == 1) \
    .drop("rn", "lat3", "lon3")

# Enrichissement par carreau proche (jointure approx sur arrondi 2 decimales + broadcast)
df_clubs_dedup = df_clubs_dedup.withColumn("club_id", F.monotonically_increasing_id()) \
    .withColumn("lat2", F.round("latitude", 2)) \
    .withColumn("lon2", F.round("longitude", 2))

df_carreaux_small = df_dash_carreaux_full.select(
    F.round("latitude", 2).alias("lat2"),
    F.round("longitude", 2).alias("lon2"),
    "score_final", "score_accessibilite", "score_revenu", "prix_median_m2", "indice_demande_trends",
    "part_cible_padel", "ind_snv", "nb_clubs_5km", "code_departement"
)

df_club_match = df_clubs_dedup.join(
    F.broadcast(df_carreaux_small),
    on=["lat2", "lon2"],
    how="left"
)

# Si plusieurs matches, garder le meilleur score_final
w_match = Window.partitionBy("club_id").orderBy(F.col("score_final").desc_nulls_last())
df_club_match = df_club_match.withColumn("rn_match", F.row_number().over(w_match)) \
    .filter(F.col("rn_match") == 1) \
    .drop("rn_match", "lat2", "lon2")

df_dash_clubs = df_club_match.withColumn(
    "zone_saturee",
    F.when(F.col("nb_clubs_5km") > 5, F.lit(True)).otherwise(F.lit(False))
).withColumn(
    "score_zone_implantation",
    F.coalesce(F.col("score_final"), F.lit(0.0))
).select(
    "nom", "type", "commune", "latitude", "longitude",
    "nombre_de_courts", "source", "departement_code",
    "source_principale", "score_zone_implantation",
    F.col("score_accessibilite").alias("score_accessibilite_zone"),
    F.col("ind_snv").alias("ind_snv_zone"),
    F.col("prix_median_m2").alias("prix_median_m2_zone"),
    F.col("indice_demande_trends").alias("indice_demande_trends_zone"),
    F.col("part_cible_padel").alias("part_cible_padel_zone"),
    "zone_saturee"
)

# Remplacement des boucles withColumn par un unique fillna() (optimisation)
cols_clubs_zeros = [
    "score_zone_implantation", "score_accessibilite_zone", "ind_snv_zone", 
    "prix_median_m2_zone", "indice_demande_trends_zone", "part_cible_padel_zone"
]
cols_clubs_na = ["nom", "type", "commune", "source", "departement_code"]

df_dash_clubs = df_dash_clubs.fillna(
    {**{c: 0.0 for c in cols_clubs_zeros}, **{c: "N/A" for c in cols_clubs_na}}
)

df_dash_clubs.write.mode("overwrite").parquet(path_dash_clubs_parquet)
df_dash_clubs.coalesce(1).write.mode("overwrite").option("header", True).csv(path_dash_clubs_csv)
count_dash_clubs = spark.read.parquet(path_dash_clubs_parquet).count()
print(f"[4/7] dash_clubs : {count_dash_clubs:,} lignes")
print(f"Verification write parquet OK -> {path_dash_clubs_parquet}")

# ===== Cell 101 | markdown =====
# ### 7.4 – Fichier de métadonnées JSON
#
# Cette cellule génère un résumé global des sorties. Elle documente les volumes, les statistiques de score et les principaux classements.
#
# Illustration réelle observée: volume total des tables Dash Ready autour de 110 Mo.

# ===== Cell 102 | code =====
"""
ETAPE 7 - CELLULE 3/3
Generation de dash_metadata.json :
- Volumes globaux, stats de score, tops departements/communes.
- Taille des datasets produits.
- Recapitulatif final imprime.
"""

import os
import json
from datetime import datetime, timezone
import pyspark.sql.functions as F

# Paths des tables produites
path_dash_carreaux_full = os.path.join(OUTPUT_DASH, "dash_carreaux_full")
path_dash_communes_agg = os.path.join(OUTPUT_DASH, "dash_communes_agg")
path_dash_top_zones_parquet = os.path.join(OUTPUT_DASH, "dash_top_zones.parquet")
path_dash_clubs_parquet = os.path.join(OUTPUT_DASH, "dash_clubs.parquet")
path_dash_transport = os.path.join(OUTPUT_DASH, "dash_transport.parquet")
path_dash_roads = os.path.join(OUTPUT_DASH, "dash_roads.parquet")
path_dash_deps_parquet = os.path.join(OUTPUT_DASH, "dash_departements_stats.parquet")
path_dash_metadata = os.path.join(OUTPUT_DASH, "dash_metadata.json")

# Lecture tables
df_carreaux = spark.read.parquet(path_dash_carreaux_full)
df_communes = spark.read.parquet(path_dash_communes_agg)
df_top_zones = spark.read.parquet(path_dash_top_zones_parquet)
df_clubs = spark.read.parquet(path_dash_clubs_parquet)
df_deps = spark.read.parquet(path_dash_deps_parquet)

# Helper taille

def _path_size_bytes(path):
    if not os.path.exists(path):
        return 0
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _, files in os.walk(path):
        for fn in files:
            fp = os.path.join(root, fn)
            if os.path.isfile(fp):
                total += os.path.getsize(fp)
    return total


# Volumes
nb_carreaux_total = df_carreaux.count()
nb_communes = df_communes.count()
nb_departements = df_deps.select("code_departement").distinct().count()
nb_clubs = df_clubs.count()
nb_top_zones = df_top_zones.count()

# Stats score_final
aqq = df_carreaux.agg(
    F.min("score_final").alias("min_v"),
    F.max("score_final").alias("max_v"),
    F.expr("percentile_approx(score_final, array(0.5, 0.9, 0.99), 10000)").alias("q")
).collect()[0]

score_final_min = float(aqq["min_v"]) if aqq["min_v"] is not None else 0.0
score_final_max = float(aqq["max_v"]) if aqq["max_v"] is not None else 0.0
score_final_median = float(aqq["q"][0]) if aqq["q"] is not None else 0.0
score_final_p90 = float(aqq["q"][1]) if aqq["q"] is not None else 0.0
score_final_p99 = float(aqq["q"][2]) if aqq["q"] is not None else 0.0

# Tops
top5_departements = [
    r["code_departement"]
    for r in df_deps.orderBy(F.desc("score_final_p90")).select("code_departement").limit(5).collect()
]

top5_communes = [
    r["code_commune_insee"]
    for r in df_communes.orderBy(F.desc("score_final_max")).select("code_commune_insee").limit(5).collect()
]

# Infos tables
tables_info = {
    "dash_carreaux_full": {
        "path": path_dash_carreaux_full,
        "nb_lignes": int(nb_carreaux_total),
        "taille_mo": _bytes_to_mb(_path_size_bytes(path_dash_carreaux_full)),
    },
    "dash_communes_agg": {
        "path": path_dash_communes_agg,
        "nb_lignes": int(nb_communes),
        "taille_mo": _bytes_to_mb(_path_size_bytes(path_dash_communes_agg)),
    },
    "dash_top_zones": {
        "path": path_dash_top_zones_parquet,
        "nb_lignes": int(nb_top_zones),
        "taille_mo": _bytes_to_mb(_path_size_bytes(path_dash_top_zones_parquet)),
    },
    "dash_clubs": {
        "path": path_dash_clubs_parquet,
        "nb_lignes": int(nb_clubs),
        "taille_mo": _bytes_to_mb(_path_size_bytes(path_dash_clubs_parquet)),
    },
    "dash_transport": {
        "path": path_dash_transport,
        "nb_lignes": int(spark.read.parquet(path_dash_transport).count()),
        "taille_mo": _bytes_to_mb(_path_size_bytes(path_dash_transport)),
    },
    "dash_roads": {
        "path": path_dash_roads,
        "nb_lignes": int(spark.read.parquet(path_dash_roads).count()),
        "taille_mo": _bytes_to_mb(_path_size_bytes(path_dash_roads)),
    },
    "dash_departements_stats": {
        "path": path_dash_deps_parquet,
        "nb_lignes": int(nb_departements),
        "taille_mo": _bytes_to_mb(_path_size_bytes(path_dash_deps_parquet)),
    },
}

metadata = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "volumes": {
        "nb_carreaux_total": int(nb_carreaux_total),
        "nb_communes": int(nb_communes),
        "nb_departements": int(nb_departements),
        "nb_clubs": int(nb_clubs),
        "nb_top_zones": int(nb_top_zones),
    },
    "score_stats": {
        "score_final_min": score_final_min,
        "score_final_max": score_final_max,
        "score_final_median": score_final_median,
        "score_final_p90": score_final_p90,
        "score_final_p99": score_final_p99,
    },
    "top5_departements": top5_departements,
    "top5_communes": top5_communes,
    "tables": tables_info,
}

with open(path_dash_metadata, "w", encoding="utf-8") as f:
    json.dump(metadata, f, ensure_ascii=False, indent=2)

# Recap final
nb_tables = 7
taille_totale_mo = sum(v.get("taille_mo", 0.0) for v in tables_info.values())
top_dept = top5_departements[0] if top5_departements else "N/A"
top_commune = top5_communes[0] if top5_communes else "N/A"

print(f" {nb_tables} tables Dash-ready generees")
print(f" Volume total : {taille_totale_mo:.0f} Mo")
print(f" Top departement : {top_dept}")
print(f" Meilleure commune : {top_commune}")
print(f" Metadata JSON : {path_dash_metadata}")

# ===== Cell 103 | markdown =====
# ---
# ## Visualisation – Carte interactive des clubs Padel
#
# Cette partie montre le résultat de tout le pipeline sous forme visuelle. Les filtres permettent d'expliquer les résultats simplement, en observant l'effet immédiat sur la carte.

# ===== Cell 104 | markdown =====
# La carte interactive est la traduction visuelle du travail précédent. Quand un filtre est modifié, la réponse de la carte permet d'expliquer immédiatement l'effet d'une hypothèse.
#
# Cette lecture est utile pour un rapport, car elle relie directement les données préparées et l'interprétation métier.

# ===== Cell 105 | markdown =====
# ### Logique de création de la carte Plotly
#
# La cellule de carte lit les données clubs préparées plus haut, puis construit les points, les couleurs, les tailles et les infobulles.
#
# Exemple réel observé: 18 637 clubs dans le CSV utilisé par la carte.

# ===== Cell 106 | markdown =====
# La cellule suivante construit les points cartographiques à partir de la table clubs. La taille et la couleur des points viennent des colonnes numériques, tandis que les infobulles reprennent les informations textuelles.
#
# Illustration réelle observée: 18 637 clubs affichables dans la source de la carte.

# ===== Cell 107 | code =====
# Carte interactive Plotly des clubs de padel - version fusionnee (preparation + UI finale)

from pathlib import Path
import math
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import ipywidgets as widgets
from IPython.display import display, clear_output


def resolve_csv_path() -> Path:
    """Trouve le premier fichier part-*.csv dans dash_clubs.csv/ quel que soit l'UUID Spark."""
    search_roots = [
        Path("/home/jovyan/work"),
        Path.cwd(),
        Path("/workspaces"),
        Path("/workspace"),
    ]
    for root in search_roots:
        if not root.exists():
            continue
        for candidate_dir in root.rglob("dash_clubs.csv"):
            if candidate_dir.is_dir():
                parts = sorted(candidate_dir.glob("part-*.csv"))
                if parts:
                    return parts[0].resolve()
        for candidate_file in root.rglob("dash_clubs.csv"):
            if candidate_file.is_file():
                return candidate_file.resolve()
    raise FileNotFoundError(
        "Aucun fichier dash_clubs.csv trouve. "
        "Verifie que l'etape 7 (ecriture dash_clubs.csv) s'est bien executee."
    )


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


def _zoom_from_bounds(lat_min, lat_max, lon_min, lon_max):
    lat_span = max(0.12, float(lat_max - lat_min) * 1.18)
    lon_span = max(0.12, float(lon_max - lon_min) * 1.18)
    z_lon = math.log2(360.0 / lon_span)
    z_lat = math.log2(170.0 / lat_span)
    return max(3.0, min(12.0, min(z_lon, z_lat) - 0.25))


CSV_PATH = resolve_csv_path()
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

score_min = float(np.nanmin(base["score_zone_implantation"]))
score_max = float(np.nanmax(base["score_zone_implantation"]))
global_min = float(base["score_zone_implantation"].min())
global_max = float(base["score_zone_implantation"].max())
if np.isfinite(global_min) and np.isfinite(global_max) and global_min == global_max:
    eps = max(abs(global_min) * 0.01, 1e-6)
    global_min -= eps
    global_max += eps

FRANCE_CENTER_LAT = 46.6
FRANCE_CENTER_LON = 2.3
FRANCE_ZOOM = 5.0
view_state = {"center_lat": FRANCE_CENTER_LAT, "center_lon": FRANCE_CENTER_LON, "zoom": FRANCE_ZOOM}
ui_state = {"suspend": False}

map_out = widgets.Output(layout=widgets.Layout(width="100%", height="1050px"))


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


# Reagencement UI: departements a gauche, carte a droite, autres filtres en bas

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

w_ui_stamp = widgets.HTML("<div style='font-size:12px; color:#4b5563; margin-bottom:2px;'>Mise a jour UI activee</div>")


def _selected_deps():
    return {code for code, cb in dep_checkboxes.items() if cb.value}


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

w_reset_deps.on_click(_on_reset)
for cb in dep_checkboxes.values():
    cb.observe(_on_deps_change, names="value")
for w in [w_type, w_score, w_mode, w_commune, w_zone_saturee, w_price, w_trends]:
    w.observe(_render, names="value")

_render()
