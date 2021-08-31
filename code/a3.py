from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

spark = SparkSession(sc)


''' 3) On average, how many tasks compose a job? '''

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

taskEventsDf = spark.read.schema(taskEventsSchema).csv("../data/task_events/*.csv.gz")

numOfTasksPerJobDf = taskEventsDf.select(
    "job_id",
    "task_index"
).distinct().groupBy("job_id").count()
pandas_dataframe_0 = numOfTasksPerJobDf.toPandas()

# Here we select column ’job id’ and ’task_index’ from taskEventsDf dataframe; then return a new dataframe
# containing the distinct elements in this dataframe; finally group by ’job id’, forming a grouped format data,
# and count number of ’task_index’ under each certain ’job_id'

avgNumOfTasksPerJob = numOfTasksPerJobDf.select(
    F.mean("count").alias("avg")
).collect()[0]["avg"]
# we select a column from numOfTasksPerJobDf frame such that it is created by applying the pyspark.sql.functions.mean on
# the extracted aggregation column "count" of dataframe numOfTasksPerJobDf and name this column as "avg", and finally we
# extract the mean result.

print("The avarage number of tasks per job is: ", round(avgNumOfTasksPerJob))
