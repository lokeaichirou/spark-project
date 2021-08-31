from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

spark = SparkSession(sc)

# 1) What is the distribution of the machines according to their CPU capacity?

# machine_events schema
machineEventsSchema = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField("machine_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("platfrom_id", StringType(), True),
        StructField("capacity_cpu", FloatType(), True),
        StructField("capacity_memory", FloatType(), True),
    ]
)

# machine_events df
machineEventsDf = spark.read.schema(machineEventsSchema).csv(
    "../data/machine_events/*.csv.gz"
)

pandas_Data_Frame_1 = machineEventsDf.toPandas()

cpuCapacityCountDf = machineEventsDf.select("machine_id", "capacity_cpu").distinct().where(F.col("capacity_cpu").isNotNull()).groupBy("capacity_cpu").count()

cpuCapacityCountDf = cpuCapacityCountDf.toPandas()
print(cpuCapacityCountDf.head(10))

print('finished')