#!/bin/sh
#
## Simple script to simplify having to delete Hadoop files manually before every spark submit

hadoop fs -rm -r /dataset/DT_ratings
hadoop fs -rm -r /dataset/DT_movies

hadoop fs -cp /dataset/DT_ratings.tmp /dataset/DT_ratings
hadoop fs -cp /dataset/DT_movies.tmp /dataset/DT_movies

#run job
spark-submit --master yarn \
--conf "spark.driver.extraJavaOptions=-Dhadoop.root.logger=WARN,console" \
--num-executors 3 \
--executor-cores 4 \
$HOME/code/sparkster.py --title "3exec, 4cores" --cache

