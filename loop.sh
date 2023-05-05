#!/bin/bash

# This script was used to simplify benchmarking. Submits the app with different settings 

# Loop 10 times
function run_spark_app() {
	num_executors=$1
	num_cores=$2
	options=$3
	echo $num_executors
	echo $num_cores
	echo $options

	for i in {1..3}
	do
		# Set the name of the job
		job_name="$num_executors exec, $num_cores cores (run $i), $options, DT_ratings included, but at night"

		# Submit the Spark application with the given flags
		spark-submit \
			--master yarn \
			--conf "spark.driver.extraJavaOptions=-Dhadoop.root.logger=WARN,console" \
			--conf "spark.app.name=$job_name" \
			--num-executors $num_executors \
			--executor-cores $num_cores \
			$HOME/code/sparkster.py --title "$job_name" $options 

		hadoop fs -rm -r /dataset/DT_movies
		hadoop fs -rm -r /dataset/DT_ratings

	done
}

# Call function, first parameter is number of executors, second parameter number of cores, then pass in flags --cache or --udf if those are wanted

# run_spark_app(num_executor, num_cores, options(--cache, --udf))
run_spark_app 1 4 --cache
run_spark_app 2 4 "--cache --udf"
run_spark_app 3 3 --udf
run_spark_app 3 4
run_spark_app 4 3
run_spark_app 5 2
run_spark_app 6 2
run_spark_app 8 1
run_spark_app 10 1
run_spark_app 12 1
run_spark_app 2 1 "--cache --udf" 
run_spark_app 2 1 "--cache" 
