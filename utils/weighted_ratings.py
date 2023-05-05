from delta import DeltaTable
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F

def calculate_weighted_rating(ratings_df, movie_deltaTable: DeltaTable, CACHE: bool):
# Aggegrate ratings_df to get sum and count
    if CACHE:
        movie_ratings_df = ratings_df.groupBy("movieId").agg(
            F.sum("rating").alias("vote_sum_new"), F.count("rating").alias("vote_count_new")).select("movieId", "vote_sum_new", "vote_count_new").cache()
    else:
        movie_ratings_df = ratings_df.groupBy("movieId").agg(
            F.sum("rating").alias("vote_sum_new"), F.count("rating").alias("vote_count_new")).select("movieId", "vote_sum_new", "vote_count_new")

    if CACHE:
        movie_deltaTableDF = movie_deltaTable.toDF().cache()
    else:
        movie_deltaTableDF = movie_deltaTable.toDF()

# Join the two dataframes based on the 'id' column

    joined_df = movie_deltaTableDF.drop("title", "genre").join(movie_ratings_df, ['movieId'], 'outer')

    df_with_sums = joined_df.withColumn("vote_sum", F.coalesce(joined_df["vote_sum_new"], F.lit(0)) + F.coalesce(joined_df["vote_sum"], F.lit(0))) \
    .withColumn("vote_count", F.coalesce(joined_df["vote_count_new"], F.lit(0)) + F.coalesce(joined_df["vote_count"], F.lit(0))) \
    .drop("vote_sum_new", "vote_count_new")
# movieID | vote_avg | vote_count | vote_sum
    df_with_sums = df_with_sums.withColumn("vote_average", df_with_sums["vote_sum"]/df_with_sums["vote_count"])


# Creating funciton, and calculating C
    C = df_with_sums.select(F.mean(F.col("vote_sum")/F.col("vote_count"))).first()[0]
    def weighted_rating(vote_count, vote_average, m=1000, C:float=C):
        return (vote_count/(vote_count+m) * vote_average) + (m/(m+vote_count) * C)

# create new column with weighted rating
    return df_with_sums.withColumn("weighted_rating", weighted_rating(F.col("vote_count"), F.col("vote_average")))
