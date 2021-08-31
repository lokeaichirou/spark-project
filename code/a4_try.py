from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

spark = SparkSession(sc)

'''4) What can you say about the relation between the scheduling class of a job, the scheduling
class of its tasks, and their priority?'''

jobEventsSchema = StructType(
    [
        StructField("timestamp", IntegerType(), True),
        StructField("missing_info", StringType(), True),
        StructField("job_id", IntegerType(), True),
        StructField("event_type", IntegerType(), True),
        StructField("user_name", StringType(), True),
        StructField("job_scheduling_class", IntegerType(), True),
        StructField("job_name", StringType(), True),
        StructField("logical_job_name", StringType(), True),
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
        StructField("task_scheduling_class", IntegerType(), True),
        StructField("priority", IntegerType(), True),
        StructField("cpu_request", FloatType(), True),
        StructField("memeory_request", FloatType(), True),
        StructField("disk_space_request", FloatType(), True),
        StructField("machine_restrictions", BooleanType(), True),
    ]
)

jobEventsDf = spark.read.schema(jobEventsSchema).csv("../data/job_events_1/*.csv.gz")
taskEventsDf = spark.read.schema(taskEventsSchema).csv("../data/task_events_1/*.csv.gz")

job_scheduling_class_Df = (
    jobEventsDf.select("job_id", "job_scheduling_class").distinct()
    .where(F.col("job_id").isNotNull() & F.col("job_scheduling_class").isNotNull())
    .sort("job_id")
)
D1 = job_scheduling_class_Df.toPandas()

task_scheduling_class_and_priority_Df = (
    taskEventsDf.select("job_id", "task_scheduling_class", "priority").distinct()
    .where(F.col("task_scheduling_class").isNotNull() & F.col("priority").isNotNull() & F.col("job_id").isNotNull())
    .sort("job_id")
)
D2 = task_scheduling_class_and_priority_Df.toPandas()

new = job_scheduling_class_Df.join(
    task_scheduling_class_and_priority_Df, ["job_id"]
).sort("job_id")
D3 = new.toPandas()

correlation = (
    task_scheduling_class_and_priority_Df.drop("job_id")
    .agg(
        F.corr(F.col("task_scheduling_class"), F.col("priority"))
    ).alias("correlation")
)
D4 = correlation.toPandas()

#print('correlation = ', correlation)

print('')
