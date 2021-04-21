# DataLakeWithSpark

## Overview

The Sparkify data warehouse has grown and should be transferred to a data lake. The source and destination for the data is Amazon S3.
For this purpose an ETL pipeline should be developed to import the JSON files into the data lake hosted on S3.

## Description

For the ETL pipeline we are using Apache Spark and hadoop-aws library. The Source Data resides in two directories:

| Path                        | Description                                      |
|-----------------------------|--------------------------------------------------|
| s3://udacity-dend/song_data | JSONs containing songs and artists data          |
| s3://udacity-dend/log_data  | JSONs containing users, times and songplays data |

## File list

| Name      | Description                          |
|-----------|--------------------------------------|
| dl.cfg    | File with AWS credentials            |
| etl.ipynb | notebook to develop the etl pipeline |
| etl.py    | etl pipeline                         |
| README.md | this documentation                   |

## Running the project

1. clone this repository
2. run `pip install pyspark findspark`
3. edit dl.cfg and provide the AWS credentials
4. edit output_data variable in etl.py script
5. run `python etl.py`