"""
Auto-generated stage script from padelspot.ipynb.
Stage 1: Données DVF (Demandes de Valeurs Foncières)
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

# ===== Stage 1: Données DVF (Demandes de Valeurs Foncières) =====

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

df_dvf_raw, has_gps = load_dvf_raw(spark)
df_dvf_raw.printSchema()

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

df_dvf_clean = clean_dvf(df_dvf_raw, has_gps)
print(f'Transactions DVF nettoyees : {df_dvf_clean.count()}')
df_dvf_clean.select(
    'code_departement', 'code_commune', 'valeur_fonciere', 'surface_reelle_bati', 'prix_m2'
).show(10, truncate=False)

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

OUTPUT_DVF = '/home/jovyan/work/data/output/dvf_clean/'
df_dvf_commune = aggregate_dvf_by_commune(df_dvf_clean, OUTPUT_DVF)
df_dvf_commune.show(10, truncate=False)
