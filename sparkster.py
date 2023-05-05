
import pyspark.sql.functions as F

from delta.tables import *
import utils.schemas as schema
import argparse


####################### SET APPLICATION SETTINGS HERE #############################################
TITLE="default run"											
CACHE=False
UDF=False
################################################################################################

parser = argparse.ArgumentParser()
parser.add_argument('--title', type=str, help='title of the application', default=TITLE)
parser.add_argument('--cache', dest='cache', action='store_true', default=CACHE, help='enable caching')
parser.add_argument('--udf', dest='udf', action='store_true', default=UDF, help='enable UDF')
args = parser.parse_args()
TITLE = args.title
CACHE = args.cache
UDF = args.udf

print("title:", TITLE)
print("cache:", CACHE)
print("udf:", UDF)
if UDF:
	from utils.weighted_rating_UDF_old import *
else:
	from utils.weighted_ratings import *

# Default paths
RATINGS_CSV_PATH = "hdfs:///dataset/output_ratings/part-*"
# RATINGS_CSV_PATH = "hdfs:///dataset/test_data/ratings_small/part-*"
MOVIE_CSV_PATH = "hdfs:///dataset/output_movies/part-*"

OUTPUT_MOVIE_DT_PATH = "hdfs:/dataset/DT_movies"
OUTPUT_RATINGS_DT_PATH = "hdfs:/dataset/DT_ratings"




def merge_ratings_DT(old_delta_table: DeltaTable, new_data: DataFrame):
    old_delta_table.alias("oldData") \
    .merge(
    source=new_data.alias("newData"),
    condition=F.expr("oldData.userId = newData.userId") & F.expr("oldData.movieId = newData.movieId")) \
    .whenMatchedUpdate(set={
        "userId": F.col("newData.userId"),
        "movieId": F.col("newData.movieId"),
        "rating": F.col("newData.rating"),

    }) \
    .whenNotMatchedInsert(values={
        "userId": F.col("newData.userId"),
        "movieId": F.col("newData.movieId"),
        "rating": F.col("newData.rating"),
    }) \
    .execute()

def merge_movies_DT(old_delta_table: DeltaTable, new_data: DataFrame):
    old_delta_table.alias("oldData") \
    .merge(
    source=new_data.alias("newData"),
    condition=F.expr("oldData.movieId = newData.movieId")) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()


# Build Spark Session with correct config
spark = SparkSession.builder.appName(TITLE) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()


# Read CSV files into DataFrames with the correct schema
spark.sparkContext.setJobDescription("Read files")
if CACHE:
	ratings_df = spark.read.csv(RATINGS_CSV_PATH, schema=schema.rating_schema).cache()
else:
	ratings_df = spark.read.csv(RATINGS_CSV_PATH, schema=schema.rating_schema)

#print(ratings_df.is_cached)

# movieID | title | Genre 
if CACHE:
	movies_df = spark.read.csv(MOVIE_CSV_PATH, schema=schema.movie_schema).cache()
else:
	movies_df = spark.read.csv(MOVIE_CSV_PATH, schema=schema.movie_schema)
#print(movies_df.is_cached)
#movies_df = spark.read.csv(MOVIE_CSV_PATH, schema=schema.movie_schema)


spark.sparkContext.setJobDescription("Create Movie_DT")
if UDF:
	movies_delta_table = DeltaTable.createIfNotExists(spark).location(
    	OUTPUT_MOVIE_DT_PATH).addColumns(schema.delta_table_schema_UDF).execute()
else:
	movies_delta_table = DeltaTable.createIfNotExists(spark).location(
    	OUTPUT_MOVIE_DT_PATH).addColumns(schema.delta_table_schema).execute()

spark.sparkContext.setJobDescription("Create table")
ratings_delta_table = DeltaTable.createIfNotExists(spark).location(
    OUTPUT_RATINGS_DT_PATH).addColumns(schema.rating_schema).execute()

spark.sparkContext.setJobDescription("Calculate weighted ratings")
if CACHE:
	if UDF:
		weighted_ratings_df = calculate_weighted_rating(ratings_df).cache()
	else:
		weighted_ratings_df = calculate_weighted_rating(ratings_df, movies_delta_table, CACHE).cache()
else:
	if UDF:
		weighted_ratings_df = calculate_weighted_rating(ratings_df)
	else:
		weighted_ratings_df = calculate_weighted_rating(ratings_df, movies_delta_table, CACHE)
#print(weighted_ratings_df.is_cached)

# join dataframes on movieId
spark.sparkContext.setJobDescription("Join tables")
if CACHE:
	output_movies_df = movies_df.join(weighted_ratings_df, on="movieId", how="left").cache()
else:
	output_movies_df = movies_df.join(weighted_ratings_df, on="movieId", how="left")


spark.sparkContext.setJobDescription("Merge new Delta rows")
merge_ratings_DT(ratings_delta_table, new_data=ratings_df)

spark.sparkContext.setJobDescription("Merge new Movie_DT")
merge_movies_DT(movies_delta_table, new_data=output_movies_df)
