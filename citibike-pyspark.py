
"""
Start-up command
pyspark --num-executors 16 --driver-memory 4g --executor-memory 8g --packages graphframes:graphframes:0.1.0-spark1.6 < script.py
"""


from graphframes import *
from functools import reduce
from pyspark.sql.functions import col, lit, when


df = spark.read.csv("/user/conorquigley/citibike/*",header=True)
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

df_stations.groupBy("station_id").count().sort("count",ascending=False).show(25)