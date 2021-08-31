from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

spark = SparkSession(sc)

""" 
Are the tasks that request the more resources the one that consume the more resources?
"""

taskUsageSchema = StructType(
    [
        StructField("start_time", LongType(), True),
        StructField("end_time", LongType(), True),
        StructField("job_id", StringType(), True),
        StructField("task_index", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("cpu_rate", FloatType(), True),
        StructField("canonical_memory_usage", FloatType(), True),
    ]
)

taskEventsSchema = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField("missing_info", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("task_index", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("scheduling_class", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("cpu_request", FloatType(), True),
        StructField("memeory_request", FloatType(), True),
        StructField("disk_space_request", FloatType(), True),
        StructField("machine_restrictions", FloatType(), True),
    ]
)

taskEventsDf = (
    spark.read.schema(taskEventsSchema)
    .csv("../data/task_events_1/*.csv.gz")
    .where(F.col("cpu_request").isNotNull() & F.col("memeory_request").isNotNull())
)
# Load tasks_events into dataframe and preprocess the data to remove rows that have null values in columns of interest


taskUsageDf = (
    spark.read.schema(taskUsageSchema)
    .csv("../data/task_usage_1/*.csv.gz")
    .where(F.col("cpu_rate").isNotNull() & F.col("canonical_memory_usage").isNotNull())
)
# Load task_usage into dataframe and preprocess the data to remove rows that have null values in columns of interest

resourcesRequestedPerTaskDf = (
    taskEventsDf.select(
        F.concat_ws("_", F.col("job_id"), F.col("task_index")).alias("task_id"),
        "cpu_request",
        "memeory_request",
    )
    .groupBy("task_id")
    .agg(
        F.max("cpu_request").alias("cpu_request"),
        F.max("memeory_request").alias("memeory_request"),
    )
)

resourcesConsumedPerTaskDf = (
    taskUsageDf.select(
        F.concat_ws("_", F.col("job_id"), F.col("task_index")).alias("task_id"),
        "cpu_rate",
        "canonical_memory_usage",
    )
    .groupBy("task_id")
    .agg(
        F.sum("cpu_rate").alias("total_cpu_usage"),
        F.sum("canonical_memory_usage").alias("total_memory_usage"),
    )
)

requestedConsumedDf = resourcesRequestedPerTaskDf.join(
    resourcesConsumedPerTaskDf, ["task_id"]
)

memoryRequestedConsumedDf = (
    requestedConsumedDf.select("task_id", "memeory_request", "total_memory_usage")
    .groupBy("memeory_request")
    .agg(F.avg("total_memory_usage").alias("avg_memory_usage"))
    .sort("memeory_request")
)

cpuRequestedConsumedDf = (
    requestedConsumedDf.select("task_id", "cpu_request", "total_cpu_usage")
    .groupBy("cpu_request")
    .agg(F.avg("total_cpu_usage").alias("avg_cpu_usage"))
    .sort("cpu_request")
)

memoryRequestedConsumedDf.coalesce(1).write.csv(
    "../data/results/analysis7/mem_requested_consumed", header=True, mode="overwrite"
)
cpuRequestedConsumedDf.coalesce(1).write.csv(
    "../data/results/analysis7/cpu_requested_consumed", header=True, mode="overwrite"
)
