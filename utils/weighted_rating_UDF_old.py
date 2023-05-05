from pyspark.sql.types import FloatType
import pyspark.sql.functions as F


def calculate_weighted_rating(ratings_df):
    # Find V, R and C for formula, and create MovieDF
    # movieID | vote_avg | vote_count
    movie_ratings_df = ratings_df.groupBy("movieId").agg(
        F.avg("rating").alias("vote_average"), F.count("rating").alias("vote_count")).cache()

    C = movie_ratings_df.select(F.mean("vote_average")).first()[0]

    # Creating funciton, and user defined function UDF:
    def weighted_rating(vote_count, vote_average, m=1000, C=C):
        return (vote_count/(vote_count+m) * vote_average) + (m/(m+vote_count) * C)

    weighted_rating_udf = F.udf(weighted_rating, FloatType())

    # Add column weighted_rating
    # movieID | title | Genre | vote_avg | vote_count | weighted_rating
    return  movie_ratings_df.withColumn("weighted_rating", weighted_rating_udf("vote_count", "vote_average"))
    # weighted_ratings_df = movie_ratings_df.withColumn(
    #     "weighted_rating", weighted_rating_udf("vote_count", "vote_average"))
