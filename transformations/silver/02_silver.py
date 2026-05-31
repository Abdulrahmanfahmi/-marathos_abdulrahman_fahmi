from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.helpers import get_table, write_delta, add_dense_rank_id

def build_silver(spark: SparkSession) -> None:
    df = get_table("marathos.bronze.ultra_marathon_raw")
    print(f"Rows in bronze: {df.count()}")

    # Skapa distance_unit kolumn
    df = df.withColumn("distance_unit",
        F.when(F.col("event_distance_length").endswith("km"), "km")
         .when(F.col("event_distance_length").endswith("mi"), "mi")
         .when(F.col("event_distance_length").endswith("h"), "h")
         .otherwise("unknown")
    )

    # Validera och ta bort ogiltiga rader
    df = df.withColumn("is_valid",
        F.when(
            (F.col("distance_unit").isin("km", "mi")) &
            (F.col("athlete_performance").endswith("h")), True
        ).when(
            (F.col("distance_unit") == "h") &
            (~F.col("athlete_performance").endswith("h")), True
        ).otherwise(False)
    ).filter(F.col("is_valid") == True)

    # Extrahera performance_value
    df = df.withColumn("performance_value",
        F.when(
            F.col("distance_unit").isin("km", "mi"),
            F.regexp_extract(F.col("athlete_performance"), r"(\d+:\d+:\d+)", 1)
        ).otherwise(
            F.regexp_extract(F.col("athlete_performance"), r"([\d\.]+)", 1)
        )
    )

    # Skapa IDs
    df = add_dense_rank_id(df, "event_name", "event_id")
    df = add_dense_rank_id(df, "athlete_id", "athlete_id_new")

    print(f"Rows after cleaning: {df.count()}")
    write_delta(df, "marathos.silver.ultra_marathon_obt")
    print("Silver table written successfully")
