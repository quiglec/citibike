#Import necessary libraries
#from graphframes import *
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when


if __name__ == "__main__":
    #build an SparkSession using the SparkSession APIs
    spark = (SparkSession
           .builder
           .appName("CitiBike")
           .getOrCreate())

    #read CitiBike data from HDFS into
    df = spark.read.csv("/user/conorquigley/citibike/*", header = True)
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