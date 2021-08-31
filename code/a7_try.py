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
        StructField("start_time", IntegerType(), True),
        StructField("end_time", IntegerType(), True),
        StructField("job_id", IntegerType(), True),
        StructField("task_index", IntegerType(), True),
        StructField("machine_id", IntegerType(), True),
        StructField("cpu_rate", FloatType(), True),
        StructField("canonical_memory_usage", FloatType(), True),
        StructField("assigned memory usage", FloatType(), True),
        StructField("unmapped page cache", FloatType(), True),
        StructField("total page cache", FloatType(), True),
        StructField("maximum memory usage", FloatType(), True),
        StructField("disk I/O time", FloatType(), True),
        StructField("local disk space usage", FloatType(), True),
        StructField("maximum CPU rate", FloatType(), True),
        StructField("maximum disk IO time", FloatType(), True),
        StructField("cycles per instruction", FloatType(), True),
        StructField("memory accesses per instruction", FloatType(), True),
        StructField("sample portion", FloatType(), True),
        StructField("aggregation type", BooleanType(), True),
        StructField("sampled CPU usage", FloatType(), True)
    ]
)

taskEventsSchema = StructType(
    [
        StructField("timestamp", IntegerType(), True),
        StructField("missing_info", StringType(), True),
        StructField("job_id", IntegerType(), True),
        StructField("task_index", IntegerType(), True),
        StructField("machine_id", IntegerType(), True),
        StructField("event_type", IntegerType(), True),
        StructField("user_name", StringType(), True),
        StructField("task_scheduling_class", LongType(), True),
        StructField("priority", IntegerType(), True),
        StructField("cpu_request", FloatType(), True),
        StructField("memeory_request", FloatType(), True),
        StructField("disk_space_request", FloatType(), True),
        StructField("machine_restrictions", BooleanType(), True),
    ]
)
taskEventsDf = (
    spark.read.schema(taskEventsSchema)
    .csv("../data/task_events/*.csv.gz")
    .where(F.col("cpu_request").isNotNull() & F.col("memeory_request").isNotNull())
)
# Load tasks_events into dataframe and preprocess the data to remove rows that have null values in columns of interest
#Data_To_Dataframe_0 = taskEventsDf.toPandas()

taskUsageDf = (
    spark.read.schema(taskUsageSchema)
    .csv("../data/task_usage/*.csv.gz")
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
#Data_To_Dataframe_1 = resourcesRequestedPerTaskDf.toPandas()
# Getting the resources requested for each task: Using the dataframe corresponding to “task_events” we again derive
# a new column “task_id” from “job_id” & “task_index” columns and select “cpu_request” and “memory_request” columns.

# Lastly, we call distinct() on the dataframe to remove duplicate rows.

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
# Calculating the total resources (cpu, memory) consumed by a task during the observed period: Using the dataframe
# corresponding to “task_usage”, we first concatenated the the “job_id” & “task_index” columns to derive a new column
# “task_id” that has unique identifier for each task.

# Next we grouped by “task_id” and summed "cpu_rate" and “canonical_memory_usage" for each task. Up to this point we
# have a dataframe that has columns “task_id”, “total_cpu_usage” and “total_memory_usage

requestedConsumedDf = resourcesRequestedPerTaskDf.join(
    resourcesConsumedPerTaskDf, ["task_id"]
)
# Joining data: Next we joined the two dataframes that hold the total resources consumed and the resources requested by
# each task.

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
