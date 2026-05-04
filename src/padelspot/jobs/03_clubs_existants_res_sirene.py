"""
Auto-generated stage script from padelspot.ipynb.
Stage 3: Clubs existants (RES + SIRENE)
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

# ===== Stage 3: Clubs existants (RES + SIRENE) =====

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

df_res_std = load_res_padel(
    spark,
    equip_path='/home/jovyan/work/data/data-es-equipement.csv',
    install_path='/home/jovyan/work/data/data-es-installation.csv',
)
print(f'Clubs RES Padel : {df_res_std.count()}')
df_res_std.show(5, truncate=False)

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
    def lambert93_to_wgs84(x, y):
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

df_sirene_std = load_sirene_padel(
    spark,
    etab_path='/home/jovyan/work/data/StockEtablissement_utf8.parquet',
    ul_path='/home/jovyan/work/data/StockUniteLegale_utf8.parquet',
)
print(f'Clubs SIRENE Padel : {df_sirene_std.count()}')
df_sirene_std.show(5, truncate=False)

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

OUTPUT_CONCURRENCE = '/home/jovyan/work/data/output/concurrence_padel/'
df_clubs_dedup = deduplicate_padel_clubs(df_res_std, df_sirene_std, OUTPUT_CONCURRENCE)
df_clubs_dedup.groupBy('Source').count().show()
df_clubs_dedup.show(10, truncate=False)
