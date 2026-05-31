from pyspark.sql import SparkSession
from utils.schemas import BRONZE_SCHEMA
from utils.helpers import write_delta

def ingest_bronze(spark: SparkSession) -> None:
    df = spark.read.csv(
        "/Volumes/marathos/default/raw/TWO_CENTURIES_OF_UM_RACES.csv",
        header=True,
        schema=BRONZE_SCHEMA
    )
    print(f"Rows ingested: {df.count()}")
    write_delta(df, "marathos.bronze.ultra_marathon_raw")
    print("Bronze table written successfully")
