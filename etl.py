import findspark

findspark.init()
import configparser
import os
from datetime import datetime

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, date_format, dayofmonth, hour, month,
                                   udf, weekofyear, year)
from pyspark.sql.types import TimestampType
from contextlib import contextmanager
import time

config = configparser.ConfigParser()
config.read('dl.cfg')

@contextmanager
def measure_time(message='', template='Finished in {}s.'):
    """
    Measures execution time of decorated code

    Arguments:
        message: message to be shown 
        template: template for the message to be shown at the end of execution. 
                  Use {} to display time in seconds.
 
    Returns:
        None
    """
    start = time.time()

    yield

    elapsed = round(time.time() - start, 2)
    print(message + ": " + template.format(elapsed))

def create_spark_session():
    """
    Create spark session with hadoop-aws package

    Returns:
        SparkSession: created spark session
    """

    conf = SparkConf(). \
            setAppName('pyspark_aws'). \
            setMaster('local[*]'). \
            set('spark.jars.packages','org.apache.hadoop:hadoop-aws:3.1.1')

    sc=SparkContext(conf=conf)

    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    hadoopConf.set('fs.s3a.access.key', config['AWS']['AWS_ACCESS_KEY_ID'])
    hadoopConf.set('fs.s3a.secret.key', config['AWS']['AWS_SECRET_ACCESS_KEY'])

    spark=SparkSession(sc)

    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extracting songs and artists from data located as json at input_data 
    and writing it to output_data location as parquet files

    Arguments:
        spark: spark connection
        input_data: path to input data location
        output_data: path to output data location

    Returns:
        None
    """

    # get filepath to song data file
    songdata = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(songdata)
    df.createOrReplaceTempView("songdata_view")

    # extract columns to create songs table
    songs_table = spark.sql("""SELECT DISTINCT song_id, title, artist_id, year, duration
                                 FROM songdata_view
                                WHERE song_id IS NOT NULL 
                        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data+"songs_table/")

    # extract columns to create artists table
    artists_table = spark.sql("""SELECT DISTINCT 
                                        artist_id, artist_name as name, artist_location as location, 
                                        artist_latitude as lattitude, artist_longitude as longitude
                                   FROM songdata_view
                                  WHERE artist_id IS NOT NULL 
                              """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+"artists_table/")


def process_log_data(spark, input_data, output_data):
    """
    Extracting users, times and songplays from data located as json at input_data 
    and writing it to output_data location as parquet files

    Arguments:
        spark: spark connection
        input_data: path to input data location
        output_data: path to output data location

    Returns:
        None
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df.filter(df.page=='NextSong').createOrReplaceTempView("log_data_view")

    # extract columns for users table    
    users_table = spark.sql("""SELECT DISTINCT userId as user_id, firstName as first_name, 
                                      lastName as last_name, gender, level
                                 FROM log_data_view
                                WHERE userId IS NOT NULL
    """)

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+"users_table/")

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)), TimestampType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    df.createOrReplaceTempView("log_data_view")
    time_table = spark.sql("""SELECT DISTINCT
                                        datetime as start_time, 
                                        hour(datetime) as hour, 
                                        day(datetime) as day, 
                                        weekofyear(datetime) as week, 
                                        month(datetime) as month, 
                                        year(datetime) as year, 
                                        dayofweek(datetime) as weekday
                                FROM log_data_view
                                WHERE datetime IS NOT NULL  
    """)

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"time_table/")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+"songs_table/")
    song_df.createOrReplaceTempView("songs_table")

    artist_df = spark.read.parquet(output_data+"artists_table/")
    artist_df.createOrReplaceTempView("artists_table")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT monotonically_increasing_id() as songplay_id,
                                        l.datetime as start_time, year(l.datetime) as year, month(l.datetime) as month, 
                                        l.userId, l.level, l.song, s.song_id, l.artist, a.artist_id, 
                                        l.sessionId, l.location, l.userAgent
                                    FROM log_data_view l
                            LEFT JOIN songs_table s ON (s.title = l.song)
                            LEFT JOIN artists_table a ON (a.name = l.artist)   
                            """) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"songplays_table/")


def main():
    """
    Main function 
    
    Arguments:
        None
        
    Returns:
        None
    """
    with measure_time("Creating Spark Session"):
        spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/songsdb/"
    
    with measure_time("process_song_data"):
        process_song_data(spark, input_data, output_data)  
    with measure_time("process_log_data"):
        process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
