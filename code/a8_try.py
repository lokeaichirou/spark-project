from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

spark = SparkSession(sc)

""" 
Is there a relation between the amount of resource consumed by tasks and their priority?
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
        StructField("aggregation type", BooleanType, True),
        StructField("sampled CPU usage", FloatType, True)
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
        StructField("priority", IntegerType(), True),
        StructField("cpu_request", FloatType(), True),
        StructField("memeory_request", FloatType(), True),
        StructField("disk_space_request", FloatType(), True),
        StructField("machine_restrictions", FloatType(), True),
    ]
)

taskEventsDf = (
    spark.read.schema(taskEventsSchema)
    .csv("../data/task_events/*.csv.gz")
    .where(F.col("priority").isNotNull())
)

taskUsageDf = (
    spark.read.schema(taskUsageSchema)
    .csv("../data/task_usage/*.csv.gz")
    .where(F.col("cpu_rate").isNotNull())
)

priorityPerTaskDf = (
    taskEventsDf.select(
        F.concat_ws("_", F.col("job_id"), F.col("task_index")).alias("task_id"),
        "priority",
    )
    .groupBy("task_id")
    .agg(F.max("priority").alias("priority"))
)

cpuConsumedPerTaskDf = (
    taskUsageDf.select(
        F.concat_ws("_", F.col("job_id"), F.col("task_index")).alias("task_id"),
        "cpu_rate",
    )
    .groupBy("task_id")
    .agg(
        F.sum("cpu_rate").alias("total_cpu_usage"),
    )
)

priorityCpuConsumedDf = (
    priorityPerTaskDf.join(cpuConsumedPerTaskDf, ["task_id"])
    .groupBy("priority")
    .agg(F.mean("total_cpu_usage").alias("avg_cpu_consumption"))
    .sort("priority")
)

priorityCpuConsumedDf.coalesce(1).write.csv(
    "../data/results/analysis8/cpu_consumed_task_priority",
    header=True,
    mode="overwrite",
)
