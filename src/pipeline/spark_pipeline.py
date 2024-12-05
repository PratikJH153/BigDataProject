from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark
spark = (
    SparkSession.builder.appName("ChicagoNightlife")
    .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.12:4.0.1")
    .getOrCreate()
)


def clean_data(df):
    return (
        df.withColumn("clean_text", regexp_replace(col("text"), "[^a-zA-Z\\s]", ""))
        .withColumn("created_date", to_date(col("created_utc")))
        .dropDuplicates()
        .na.drop()
    )
