from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import *

# simple Spark app which just shows the delta table


spark = SparkSession.builder.appName("read delta table") \
	.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
	.getOrCreate()

# delta_table_path = "hdfs:///dataset/DT_ratings"
delta_table_path = "hdfs:///dataset/DT_movies"

sc = spark.sparkContext.setJobDescription("open delta table")
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Convert Delta table to PySpark DataFrame
delta_df = delta_table.toDF().orderBy(F.desc("weighted_rating"))

# Show DataFrame contents
delta_df.show(50)
