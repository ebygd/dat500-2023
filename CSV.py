from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

#This is only used once to transform the original dataset into .txt format

class MRJobCSV(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol
    # OUTPUT_PROTOCOL = CsvProtocol
    def mapper(self, _, line):
        
        line = line.strip()  # remove leading and trailing whitespace
        line = line.split(",")
        userId = line[0]
        movieId = line[1]
        rating = line[2]
        
        # yield modified row as key-value pair
        if userId != "userId":
            yield "", f"{userId},{movieId},{rating}"

    def reducer(self, _, rows):
        # write output rows as csv data
        for row in rows:
            yield (None, row)


if __name__ == '__main__':
    MRJobCSV.run()
