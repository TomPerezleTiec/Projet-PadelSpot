"""
Auto-generated stage script from padelspot.ipynb.
Stage 7: Préparation des exports Dash Ready
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

from pyspark.sql.types import DoubleType

from pyspark.sql.window import Window

from functools import reduce

import json

from pyspark.sql import SparkSession, Window

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, FloatType, ShortType, IntegerType

import geopandas as gpd

from pyspark.sql import Window

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, BooleanType

from datetime import datetime, timezone

from pathlib import Path

import math

import numpy as np

import plotly.graph_objects as go

import ipywidgets as widgets

from IPython.display import display, clear_output

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

# ===== Stage 7: Préparation des exports Dash Ready =====

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
    "concurrence_padel": "/home/jovyan/work/data/output/concurrence_padel/",
    "clubs_concurrents": "/home/jovyan/work/data/output/concurrence_padel/",
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
    "/home/jovyan/work/data/output/concurrence_padel/"
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


# ============================================================
# [5/7] dash_departements_stats
# ============================================================
path_dash_deps_parquet = os.path.join(OUTPUT_DASH, "dash_departements_stats.parquet")
path_dash_deps_csv = os.path.join(OUTPUT_DASH, "dash_departements_stats.csv")

df_dash_deps = df_dash_carreaux_full.groupBy("code_departement").agg(
    F.count("*").alias("nb_carreaux"),
    F.avg("score_final").alias("score_final_moyen"),
    F.expr("percentile_approx(score_final, 0.9)").alias("score_final_p90"),
    F.avg("score_concurrence").alias("score_concurrence_moyen"),
    F.avg("score_accessibilite").alias("score_accessibilite_moyen"),
    F.avg("score_immobilier").alias("score_immobilier_moyen"),
    F.avg("score_revenu").alias("score_revenu_moyen"),
    F.avg("part_cible_padel").alias("part_cible_padel_moyen"),
    F.avg("indice_demande_trends").alias("indice_demande_trends_moyen"),
    F.avg("prix_median_m2").alias("prix_median_m2_moyen"),
    F.avg("ind_snv").alias("ind_snv_moyen"),
    F.avg("nb_clubs_5km").alias("nb_clubs_5km_moyen"),
).fillna(
    {
        "code_departement": "N/A",
        "nb_carreaux": 0,
        "score_final_moyen": 0.0,
        "score_final_p90": 0.0,
        "score_concurrence_moyen": 0.0,
        "score_accessibilite_moyen": 0.0,
        "score_immobilier_moyen": 0.0,
        "score_revenu_moyen": 0.0,
        "part_cible_padel_moyen": 0.0,
        "indice_demande_trends_moyen": 0.0,
        "prix_median_m2_moyen": 0.0,
        "ind_snv_moyen": 0.0,
        "nb_clubs_5km_moyen": 0.0,
    }
)

df_dash_deps.write.mode("overwrite").parquet(path_dash_deps_parquet)
df_dash_deps.coalesce(1).write.mode("overwrite").option("header", True).csv(path_dash_deps_csv)
count_dash_deps = spark.read.parquet(path_dash_deps_parquet).count()
print(f"[5/7] dash_departements_stats : {count_dash_deps:,} lignes")
print(f"Verification write parquet OK -> {path_dash_deps_parquet}")

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


def _safe_parquet_count(path):
    if not os.path.exists(path):
        return 0
    return int(spark.read.parquet(path).count())


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
        "nb_lignes": _safe_parquet_count(path_dash_transport),
        "taille_mo": _bytes_to_mb(_path_size_bytes(path_dash_transport)),
    },
    "dash_roads": {
        "path": path_dash_roads,
        "nb_lignes": _safe_parquet_count(path_dash_roads),
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
