from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

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
# Create a dataframe by reading csv file

cpuCapacityCountDf = machineEventsDf.select("machine_id", "capacity_cpu").distinct().where(F.col("capacity_cpu").isNotNull()).groupBy("capacity_cpu").count()

# Select column 'machine_id' and 'capacity_cpu' from machineEventsDf dataframe; then return a new dataframe containing the distinct elements in this dataframe;
# then filter out rows whose 'capacity_cpu' is not null; finally group by 'capacity_cpu', forming a grouped format data, and count number of 'machine_id' under
# each certain 'capacity_cpu'.

cpuCapacityCountDf.coalesce(1).write.csv("../data/output/analysis1/machine_cpu_capacity_dist", header=True, mode="overwrite")

print('finished')
