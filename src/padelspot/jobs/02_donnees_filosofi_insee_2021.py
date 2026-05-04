"""
Auto-generated stage script from padelspot.ipynb.
Stage 2: Données Filosofi (INSEE 2021)
"""

from __future__ import annotations

# Shared pipeline paths
OUTPUT_DVF = "/home/jovyan/work/data/output/dvf_clean/"
OUTPUT_FILOSOFI = "/home/jovyan/work/data/output/filosofi_clean/"
OUTPUT_CONCURRENCE = "/home/jovyan/work/data/output/concurrence_padel/"
output_path_access = "/home/jovyan/work/data/output/accessibilite_clean/"
OUTPUT_TRENDS = "/home/jovyan/work/data/output/trends_joined/"
OUTPUT_DASH = "/home/jovyan/work/data/dash_ready/"

from pyspark.sql import SparkSession

import pyspark.sql.functions as F

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, IntegerType, FloatType, ShortType, ArrayType,
)

import glob

# NOTEBOOK_MAGIC: %pip install pyspark plotly ipywidgets pandas numpy anywidget osmium pyproj xgboost -q

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

spark = get_spark('PadelSpot_Pipeline', driver_memory='4g')
spark

# ===== Stage 2: Données Filosofi (INSEE 2021) =====

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

df_filosofi_raw = load_filosofi_raw(spark)
print(f'Lignes brutes Filosofi : {df_filosofi_raw.count()}')
df_filosofi_raw.printSchema()

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

df_filosofi_typed = cast_filosofi_numeric(df_filosofi_raw)
df_filosofi_clean = filter_filosofi(df_filosofi_typed)
print(f'Lignes apres nettoyage : {df_filosofi_clean.count()}')
df_filosofi_clean.select('IdINSPIRE', 'code_commune_insee', 'I_est_cr', 'Ind', 'Ind_snv', 'X_c', 'Y_c').show(10, truncate=False)

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
    def epsg3035_to_wgs84(x_series, y_series):
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

df_filosofi_geo = convert_epsg3035_to_wgs84(df_filosofi_clean)
print(f'Lignes apres conversion GPS + filtre geo : {df_filosofi_geo.count()}')
df_filosofi_geo.select('IdINSPIRE', 'Longitude', 'Latitude').show(10, truncate=False)

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
