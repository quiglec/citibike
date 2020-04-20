#Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType
from pyspark.sql.functions import udf, unix_timestamp, to_timestamp, col

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
        `birth_year` STRING, 
        `gender` STRING
        """

#Define function for standardizing timestamp fields
def replace_date(string: str):
    it = string.replace("/", "-")
    if it[:3] != "201":
        date_str = f'{it.split("-")[2][:4]}-{it.split("-")[0]}-{it.split("-")[1]}{it.split("-")[2][4:]}'
        return date_str
    else:
        return it

replace = udf(replace_date, StringType())
spark.udf.register("replace", replace)

if __name__ == "__main__":
    #Build an SparkSession using the SparkSession APIs
    spark = (SparkSession
           .builder
           .appName("CitiBike")
           .getOrCreate())

    #Read CitiBike data from HDFS into
    df = spark.read.csv("/user/clsadmin/data1/*", header = True, schema = schema)
    df1 = df.withColumn("start_time", replace(df.start_time)).withColumn("stop_time", replace(df.stop_time))
    df2 = df1.withColumn("start_time", to_timestamp(col("start_time"))).withColumn("stop_time", to_timestamp(col("stop_time")))

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

