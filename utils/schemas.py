from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

BRONZE_SCHEMA = StructType([
    StructField("year_of_event", IntegerType(), True),
    StructField("event_dates", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_distance_length", StringType(), True),
    StructField("event_number_of_finishers", IntegerType(), True),
    StructField("athlete_performance", StringType(), True),
    StructField("athlete_club", StringType(), True),
    StructField("athlete_country", StringType(), True),
    StructField("athlete_year_of_birth", DoubleType(), True),
    StructField("athlete_gender", StringType(), True),
    StructField("athlete_age_category", StringType(), True),
    StructField("athlete_average_speed", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
])
