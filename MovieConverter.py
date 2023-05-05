from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol


class MRJobCSV(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    def mapper(self, _, line):
        # read input line as csv data
        line = line.strip()  # remove leading and trailing whitespace
        line = line.split(",")
        movieId = line[0]
        title = line[1:-1]
        genres = line[-1]
        
        # yield modified row as key-value pair
        if movieId != "movieId":
            yield "", f"{movieId},{','.join(title)},{genres}"

    def reducer(self, _, rows):
        # write output rows as csv data
        for row in rows:
            yield (None, row)
            
if __name__ == '__main__':
    MRJobCSV.run()
