from pyspark.sql import DataFrame
from pyspark.sql.functions import dense_rank
from pyspark.sql.window import Window

def get_table(table_name: str):
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    return spark.table(table_name)

def write_delta(df: DataFrame, table_name: str) -> None:
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(table_name))

def add_dense_rank_id(df: DataFrame, order_col: str, id_col: str) -> DataFrame:
    w = Window.orderBy(order_col)
    return df.withColumn(id_col, dense_rank().over(w))
