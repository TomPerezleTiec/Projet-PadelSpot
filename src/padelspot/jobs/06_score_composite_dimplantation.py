"""
Auto-generated stage script from padelspot.ipynb.
Stage 6: Score composite d’implantation
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

# ===== Stage 6: Score composite d’implantation =====

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

"""
Section 6.2  Score concurrentiel (broadcast + Pandas UDF Haversine vectorisée).
- Broadcast explicite de concurrence_padel (~faible volumétrie relative).
- Jointure par buckets spatiaux + candidats voisins pour éviter un produit cartésien global.
- Calcul vectorisé des distances et agrégation: distance_club_plus_proche, nb_clubs_5km.
- score_concurrence: 1 - norm(nb_clubs_5km) + bonus si distance > 10 km.
"""
@F.pandas_udf(DoubleType())
def haversine_km(lat1, lon1, lat2, lon2):
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
