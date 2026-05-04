"""
Auto-generated stage script from padelspot.ipynb.
Stage 4: Accessibilité (OpenStreetMap)
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

# ===== Stage 4: Accessibilité (OpenStreetMap) =====

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

"""
Section 4.3  Calcul d'accessibilité par carreau INSEE.
- Charge les carreaux de l'étape 2 (filosofi_clean).
- Calcule densite_tc (arrêts TC à <=1 km).
- Calcule proximite_axe (distance minimale à un axe majeur).
- Utilise une Pandas UDF Haversine vectorisée (Arrow).
"""
import pandas as pd

@F.pandas_udf("double")
def haversine_km(lat1, lon1, lat2, lon2):
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
