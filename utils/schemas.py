from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, FloatType

rating_schema = StructType([
    StructField("userId", IntegerType(), False),
    StructField("movieId", IntegerType(), False),
    StructField("rating", DoubleType(), False),
])

movie_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genre", StringType(), True),
])

delta_table_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("vote_sum", DoubleType(), True),
    StructField("weighted_rating", DoubleType(), True),
])
delta_table_schema_UDF = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    #StructField("vote_sum", DoubleType(), True), # COMMENT THIS FOR UDF
    StructField("weighted_rating", DoubleType(), True),
])
