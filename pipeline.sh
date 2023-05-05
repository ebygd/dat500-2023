#!/bin/bash

# Input files for Hadoop 
DEFAULT_MOVIE_PATH="hdfs:///dataset/movies.txt"
DEFAULT_RATING_PATH="hdfs:///dataset/ratings.txt"
# DEFAULT_RATING_PATH="hdfs:///dataset/ratings_small.txt"

# Hdfs temporary folders saves files to this path:
OUTPUT_DIR_RATINGS="hdfs:///dataset/output_ratings"
OUTPUT_DIR_MOVIES="hdfs:///dataset/output_movies"

# Parse input arguments
while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        --input-movies)
        INPUT_MOVIES="$2"
        shift # past argument
        shift # past value
        ;;
        --input-ratings)
        INPUT_RATINGS="$2"
        shift # past argument
        shift # past value
        ;;
        *)    # unknown option
        echo "Available options:"
        echo "--input-movies <path>: path to input movies file (default: $DEFAULT_MOVIE_PATH)"
        echo "--input-ratings <path>: path to input ratings file (default: $DEFAULT_RATING_PATH)"
        echo ""
        exit 1
        ;;
    esac
done

# Set default input file paths if not provided
if [[ -z $INPUT_MOVIES ]]
then
  INPUT_MOVIES=$DEFAULT_MOVIE_PATH
  echo "No movie path provided. Using default path: $DEFAULT_MOVIE_PATH"
fi

if [[ -z $INPUT_RATINGS ]]
then
  INPUT_RATINGS=$DEFAULT_RATING_PATH
  echo "No rating path provided. Using default path: $DEFAULT_RATING_PATH"
fi

echo $INPUT_RATINGS
echo $INPUT_MOVIES

echo ""
echo "#################################"
echo "#   Hadoop Map Reduce Ratings   #"
echo "#################################"
# Run hadoop jobs
python3 Converter.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -r hadoop $INPUT_RATINGS --output-dir $OUTPUT_DIR_RATINGS --no-output

echo ""
echo "################################"
echo "#   Hadoop Map Reduce Movies   #"
echo "################################"
python3 MovieConverter.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -r hadoop $INPUT_MOVIES --output-dir $OUTPUT_DIR_MOVIES --no-output
    
echo ""
echo "###########################"
echo "#   Submitting to Spark   #"
echo "###########################"

spark-submit --master yarn \
   --conf "spark.driver.extraJavaOptions=-Dhadoop.root.logger=WARN,console" \
   --num-executors 3 \
   --executor-cores 4 \
   sparkster.py --title "Pipeline: (3exec, 4 core)" --cache


echo ""
echo "#################################"
echo "#   Deleting temp output dirs   #"
echo "#################################"
# Remove output files
hadoop fs -rm -r $OUTPUT_DIR_RATINGS && hadoop fs -rm -r $OUTPUT_DIR_MOVIES
