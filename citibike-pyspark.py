#Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType
from pyspark.sql.functions import udf, unix_timestamp, col

#Define dataframe schema
schema = """
        `trip_duration` INT, 
        `start_time` STRING, 
        `stop_time` STRING, 
        `start_station_id` STRING, 
        `start_station_name` STRING, 
        `start_station_latitude` FLOAT, 
        `start_station_longitude` FLOAT, 
        `end_station_id` STRING, 
        `end_station_name` STRING, 
        `end_station_latitude` FLOAT, 
        `end_station_longitude` FLOAT, 
        `bike_id` STRING, 
        `user_type` STRING, 
        `birth_year` INT, 
        `gender` STRING
        """

#Define function for standardizing timestamp fields
def replace_date(string: str) -> str:
    it = string.replace("/", "-")
    if it[:3] != "201":
        return it.split("-")[2][:4]+"-"+it.split("-")[0]+"-"+it.split("-")[1]+it.split("-")[2][4:]

replace = udf(lambda x: replace_date(x), StringType())

if __name__ == "__main__":
    #build an SparkSession using the SparkSession APIs
    spark = (SparkSession
           .builder
           .appName("CitiBike")
           .getOrCreate())

    #read CitiBike data from HDFS into
    df = spark.read.csv("/user/clsadmin/data/*", header = True, schema = schema)

    df1 = 
df.withColumn("start_time", unix_timestamp(replace(df.start_time),"YYYY-MM-DD HH:MM:SS").cast(TimestampType())).withColumn("stop_time", unix_timestamp(replace(df.stop_time),"YYYY-MM-DD HH:MM:SS").cast(TimestampType()))


    """
    df.cache()
    df_stations = 
            df.select(
                col("start station id").alias("station_id"),
                col("start station name").alias("station_name")
            )
            .union(
                df.select(
                    "end station id",
                    "end station name")
                )
            .distinct()

    df_stations.groupBy("station_id").count().sort("count", ascending = False).show(25)
    """

