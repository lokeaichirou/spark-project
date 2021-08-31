import sys
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import time

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

spark = SparkSession(sc)

""" 2) What is the percentage of computational power lost due to maintenance
 (a machine went ofﬂine and reconnected later)? """

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
).where(F.col("capacity_cpu").isNotNull())

# max timestamp
maxTime = machineEventsDf.select(F.max("timestamp").alias("max")).collect()[0]["max"]

eventsPerMachineDf = machineEventsDf.where(F.col("event_type") != "2").select(
    "machine_id",
    "capacity_cpu",
    F.struct("event_type", "timestamp").alias("event_type_timestamp"),
)

eventsPerMachineDf = eventsPerMachineDf.groupBy("machine_id", "capacity_cpu").agg(
    F.collect_list("event_type_timestamp").alias("events")
)


@udf(returnType=LongType())
def calculateDowntime(machineEvents):
    totalDownTime = 0
    global maxTime
    for event in machineEvents:
        if event[0] == "0":
            totalDownTime += int(event[1])
        elif event[0] == "1":
            totalDownTime -= int(event[1])
        if event[0] == "1" and event is machineEvents[-1]:
            totalDownTime += maxTime
    return totalDownTime


totalDowntimePerMachineDf = (
    eventsPerMachineDf.withColumn("total_down_time", calculateDowntime(F.col("events")))
    .where(F.col("total_down_time").isNotNull())
)

idealComputationCapacity = (
    machineEventsDf.where(F.col("capacity_cpu").isNotNull())
        .rdd.map(lambda x: x["capacity_cpu"] * maxTime)
        .sum()
)

lostComputationCapacity = totalDowntimePerMachineDf.rdd.map(
    lambda x: x["capacity_cpu"] * x["total_down_time"]
).sum()

print(
    "The percentage of computational power lost due to maintanance is:",
    round((lostComputationCapacity / idealComputationCapacity) * 100, 2),
    "%",
)
