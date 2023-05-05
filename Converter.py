from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol


class RatingConverter(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        line = line.strip()  # remove leading and trailing whitespace
        line = line.split(",")
        userId = line[0]
        movieId = line[1]
        rating = line[2]

        if userId != "userId":
            yield "", f"{userId},{movieId},{rating}"

    def reducer(self, _, rows):
        # write output rows as csv data
        for row in rows:
            yield (None, row)

if __name__ == '__main__':
    RatingConverter.run()

# python3 Converter.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -r hadoop hdfs:///dataset/ratings_small.txt --output-dir hdfs:///dataset/rating_output --no-output

# python3 MovieConverter.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -r hadoop hdfs:///dataset/movies.txt --output-dir hdfs:///dataset/movie_output --no-output
