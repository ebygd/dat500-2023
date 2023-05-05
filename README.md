# Movie Analyzer using Spark Optimization Techniques

This project is a movie analyzer that uses Spark optimization techniques to improve performance. The analyzer works with a large dataset provided by MovieLens, which is not included in this repository.

## Requirements

This project requires the following software to be installed:

- Apache Hadoop
- Apache Spark

Tested with Spark version 3.3.2
Tested with Hadoop version 3.2.1

This project was run on a cluster of 4 nodes, whereas 3 are datanodes with 8 GB RAM and 4 vCPUs. Namenode has 4 GB RAM with 4 vCPUs.

## Installation

To install and use the analyzer, follow these steps:

1. Clone this repository to your local machine using `git clone`.

2. Download the dataset from [MovieLens](https://grouplens.org/datasets/movielens/).

3. Extract the dataset and place it in HDFS, which is accessible by your Hadoop and Spark installation.

4. Run `CSV.py` to convert files from `csv` to `txt`.

6. Run the analyzer using the script `pipeline.sh --input-ratings <file> --input-movies <file>` these files should be stored in HDFS.

## Functionality

The movie analyzer implements three Spark optimization techniques to improve performance:

1. Managing Resources: This optimization ensures that resources are allocated efficiently by controlling the amount of memory used by Spark.

2. Caching: This optimization caches frequently accessed data in memory, reducing the need to read it from disk.

3. Avoiding User-Defined Functions (UDFs): This optimization uses Spark's built-in functions instead of User-Defined functions wherever possible, reducing the overhead of calling external code.

The analyzer can perform various analyses on the MovieLens dataset, such as finding the most popular movies, identifying the highest-rated movies, and computing movie ratings over time.
