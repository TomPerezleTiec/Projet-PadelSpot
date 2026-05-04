"""
Auto-generated stage script from padelspot.ipynb.
Stage 5: Demande latente (Google Trends)
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

import os

import importlib.util

import subprocess

import sys

import textwrap

import time

from IPython.display import clear_output

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

import pandas as pd

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

# ===== Stage 5: Demande latente (Google Trends) =====

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

df_trends_by_departement = explode_trends_to_departements(spark, df_trends_clean)
df_trends_by_departement.orderBy('code_departement').show(100, truncate=False)

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
