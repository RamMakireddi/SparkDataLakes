import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth,dayofweek, hour, weekofyear, date_format, from_unixtime, to_timestamp
from pyspark.sql.types import TimestampType


#config = configparser.ConfigParser()
#config.read_file(open('dl.cfg'))

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    song_data = input_data+'/song_data/*/*/*/*.json'
    df = spark.read.json(song_data)
    
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    songsParquet=output_data+"/songs.parquet"
    if os.path.exists(songsParquet):
        print("songs parquet already processed")
    else:
        songs_table.write.parquet(songsParquet)
    
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location","artist_latitude as latitude", "artist_longitude as longitude")
    artistsParquet=output_data+"/artists.parquet"
    if os.path.exists(artistsParquet):
        print("artists parquet already processed")
    else:
        artists_table.write.parquet(artistsParquet)


def process_log_data(spark, input_data, output_data):
    log_data = input_data+'/log-data/*.json'

    df = spark.read.json(log_data)
    
    df = df.filter("page='NextSong'")

    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level")
    
    usersParquet=output_data+"/users.parquet"
    if os.path.exists(usersParquet):
        print("users parquet already processed")
    else:
        users_table.write.parquet(usersParquet)

    get_timestamp = udf(lambda x: int(x/1000), )
    dft = df.withColumn("timestamp", from_unixtime(get_timestamp("ts")))
    
    get_datetime = udf(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'), TimestampType())
    logdf = dft.withColumn("datetime", get_datetime("timestamp"))
    
    time_table = logdf.selectExpr("datetime as start_time", "hour(datetime) as hour", "day(datetime) as day", "weekofyear(datetime) as week", "month(datetime) as month", "year(datetime) as year", "dayofweek(datetime) as weekday")
    
    timeParquet=output_data+"/time.parquet"
    if os.path.exists(timeParquet):
        print("time parquet already processed")
    else:
        time_table.write.parquet(timeParquet)

    song_df = spark.read.parquet(output_data+"/songs.parquet") 
    
    log_table = logdf.selectExpr("datetime as start_time", "userId as user_id", "level", "artist", "song", "sessionId as session_id", "location", "userAgent as user_agent")
    join_table=log_table.join(song_df, song_df.title==log_table.song)
    songplayid_table=join_table.withColumn("songplay_id", monotonically_increasing_id())
    songplays_table = songplayid_table.select("songplay_id","start_time", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent")

    songPlaysParquet=output_data+"/songplays.parquet"
    if os.path.exists(songPlaysParquet):
        print("songplays parquet already processed")
    else:
        songplays_table.write.parquet(songPlaysParquet)    

def main():
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://udacity-dend/spark-warehouse"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
