from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import numpy as np

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

spark = SparkSession(sc)

'''5) Do tasks with low priority have a higher probability of being evicted? '''

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

taskEventsDf = (
    spark.read.schema(taskEventsSchema)
        .csv("../data/task_events/*.csv.gz")
        .where(F.col("event_type").isNotNull() & F.col("priority").isNotNull())
)
# Load tasks_events into dataframe and preprocess the data to remove rows that have null values in columns of interest

task_Per_priority_Df = (
    taskEventsDf.select(
        F.concat_ws("_", F.col("job_id"), F.col("task_index")).alias("task_id"),
        "priority",
    )
        .groupBy("priority")
        .count()
        .withColumnRenamed("count", "number of tasks per priority")
        .sort("priority")
)
# compute the number of tasks under each certain priority level
task_Per_priority_Dataframe_toPandas = task_Per_priority_Df.toPandas()

evicted_tasks_Per_priority_Df = (
    taskEventsDf.select(
        F.concat_ws("_", F.col("job_id"), F.col("task_index")).alias("task_id"),
        "event_type",
        "priority"
    )
        .where(F.col("event_type") == 2)
        .groupBy("priority")
        .count()
        .withColumnRenamed("count", "number of evicted tasks per priority")
        .sort("priority")
)
evicted_tasks_Per_priority_Dataframe_toPandas = evicted_tasks_Per_priority_Df.toPandas()

new = task_Per_priority_Df.join(
    evicted_tasks_Per_priority_Df, ["priority"]
).sort("priority")
# compute the number of evicted tasks under each certain priority level
new_Dataframe_toPandas = new.toPandas()


@udf(returnType=FloatType())
def compute_Evicted_event_Per_priority(tasks, evicted_tasks):
    prob = evicted_tasks / tasks
    return prob


Evicted_task_Per_priority_probability_Df = (
    new.withColumn("probability of evicted_event",
                   compute_Evicted_event_Per_priority(F.col("number of tasks per priority"),
                                                      F.col("number of evicted tasks per priority")))

).drop("number of tasks per priority", "number of evicted tasks per priority")
DataFrame_To_pandas_0 = Evicted_task_Per_priority_probability_Df.toPandas()
print(DataFrame_To_pandas_0.head(20))
