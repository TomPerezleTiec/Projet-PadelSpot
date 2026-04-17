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


# ============================================================
# ÉTAPE 3 – Concurrence RES + SIRENE
# Identification et déduplification des clubs Padel.
# ============================================================

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


# ============================================================
# ÉTAPE 6 – Score Composite Final
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


# ============================================================
# ÉTAPE 7 – Préparation Dash Ready
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
