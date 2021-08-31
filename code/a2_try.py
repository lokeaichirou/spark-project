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
        StructField("machine_id", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("platfrom_id", StringType(), True),
        StructField("capacity_cpu", FloatType(), True),
        StructField("capacity_memory", FloatType(), True),
    ]
)

# machine_events df
machineEventsDf = spark.read.schema(machineEventsSchema).csv(
    "../data/machine_events/*.csv.gz"
).where(F.col("capacity_cpu").isNotNull())  # remove the row whose 'capacity_cpu' is null
Pandas_Data_Frame_0 = machineEventsDf.toPandas()

# max timestamp
maxTime = machineEventsDf.select(F.max("timestamp").alias("max")).collect()[0]["max"]


eventsPerMachineDf = machineEventsDf.where(F.col("event_type") != "2").select(
    "machine_id",
    "capacity_cpu",
    F.struct("event_type", "timestamp").alias("event_type_timestamp")
)  # remove the row whose 'event_type' is not "2"
# Create another dataframe namely 'eventsPerMachineDf' based on dataframe machineEventsDf since we would like to
# generate a new column including information of event_type and capacity_cpu.

# Since we need the information of time spent by maintenance, only the event types ADD (0) and REMOVE (1) are needed,
# so we filter out the event type of UPDATE (2) at the beginning. As the monitoring of event type is for each machine
# (machine_id), we select the column of "machine_id".

# Because the power consumption is proportional to number of CPU, and we know each machine has different number of CPUs
# (as indicated by "capacity_cpu"), we select the column of "capacity_cpu" as well.

# Because the power consumption is proportional to active time of machine, i.e. time between events  ADD (0) and
# REMOVE (1). And we use pyspark.sql.functions.struct to create a new struct column namely 'event_type_timestamp'
# containing "event_type"and "timestamp". And this new struct column is the ROW type, that is a row in the DataFrame,
# the fields can be accessed like attributes.
Pandas_Data_Frame_1 = eventsPerMachineDf.toPandas()

eventsPerMachineDf = eventsPerMachineDf.groupBy("machine_id", "capacity_cpu").agg(
    F.collect_list("event_type_timestamp").alias("events")
)
# We group by "machine_id" and "capacity_cpu" for the purpose of a newly aggregated column 'event_type_timestamp' to
# have time information of events for each machine_id. And pyspark.sql.functions.collect_list is an aggregation
# function that returns a list of objects with duplicates, then rename it as "events".
Pandas_Data_Frame_2 = eventsPerMachineDf.toPandas()


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
# The calculateDowntime function is performed on column of "events" row by row. The downtime can be computed as
# case 1(final event is ADD(0) ):
# totaldowntime = sum of timestamps at event of ADD(0) + (- sum of timestamps at event of REMOVE(1) )
# case 2(final event is REMOVE(1)):
# totaldowntime =  maxTime + sum of timestamps at event of ADD(0) + (- sum of timestamps at event of REMOVE(1) )

totalDowntimePerMachineDf = (
    eventsPerMachineDf.withColumn("total_down_time", calculateDowntime(F.col("events"))).where(F.col("total_down_time").isNotNull())
)
# Since we compute the downtime, we reject the case that do not have downtime (downtime = 0)
Pandas_Data_Frame_3 = totalDowntimePerMachineDf.toPandas()
idealComputationCapacity = machineEventsDf.where(F.col("capacity_cpu").isNotNull()).rdd.map(lambda x: x["capacity_cpu"] * maxTime).sum()
# we convert the dataframe into RDD (Resilient Distributed Datasets ) for the purpose of faster computation by the
# advantage of distributed computation; We then use map method of RDD to return a new RDD by applying a function to
# each element of this RDD; Finally we sum all elements


lostComputationCapacity = totalDowntimePerMachineDf.rdd.map(lambda x: x["capacity_cpu"] * x["total_down_time"]).sum()

print(
    "The percentage of computational power lost due to maintanance is:",
    round((lostComputationCapacity / idealComputationCapacity) * 100, 2),
    "%",
)

print('')
