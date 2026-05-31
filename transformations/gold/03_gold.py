from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.helpers import get_table, write_delta

def build_gold(spark: SparkSession) -> None:
    df = get_table("marathos.silver.ultra_marathon_obt")
    print(f"Rows in silver: {df.count()}")

    dim_event = df.select(
        "event_id", "event_name", "event_dates",
        "event_distance_length", "event_number_of_finishers",
        "year_of_event", "distance_unit"
    ).distinct()
    write_delta(dim_event, "marathos.gold.dim_event")
    print(f"dim_event: {dim_event.count()} rows")

    dim_athlete = df.select(
        F.col("athlete_id_new").alias("athlete_id"),
        "athlete_country", "athlete_gender",
        "athlete_year_of_birth", "athlete_age_category", "athlete_club"
    ).distinct()
    write_delta(dim_athlete, "marathos.gold.dim_athlete")
    print(f"dim_athlete: {dim_athlete.count()} rows")

    fct_results = df.select(
        "result_id", "event_id",
        F.col("athlete_id_new").alias("athlete_id"),
        "athlete_performance", "performance_value",
        "distance_unit", "athlete_average_speed", "year_of_event"
    )
    write_delta(fct_results, "marathos.gold.fct_results")
    print(f"fct_results: {fct_results.count()} rows")

    spark.sql("""
        CREATE OR REPLACE VIEW marathos.gold.vw_distance_events AS
        SELECT f.result_id, f.athlete_id, f.athlete_performance,
               f.performance_value, f.distance_unit, f.athlete_average_speed,
               e.event_name, e.event_distance_length, e.year_of_event,
               a.athlete_country, a.athlete_gender, a.athlete_year_of_birth
        FROM marathos.gold.fct_results f
        JOIN marathos.gold.dim_event e ON f.event_id = e.event_id
        JOIN marathos.gold.dim_athlete a ON f.athlete_id = a.athlete_id
        WHERE f.distance_unit IN ('km', 'mi')
    """)

    spark.sql("""
        CREATE OR REPLACE VIEW marathos.gold.vw_timed_events AS
        SELECT f.result_id, f.athlete_id, f.athlete_performance,
               f.performance_value, f.distance_unit, f.athlete_average_speed,
               e.event_name, e.event_distance_length, e.year_of_event,
               a.athlete_country, a.athlete_gender, a.athlete_year_of_birth
        FROM marathos.gold.fct_results f
        JOIN marathos.gold.dim_event e ON f.event_id = e.event_id
        JOIN marathos.gold.dim_athlete a ON f.athlete_id = a.athlete_id
        WHERE f.distance_unit = 'h'
    """)
    print("Gold layer complete")
