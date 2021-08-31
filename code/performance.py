from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time

sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

spark = SparkSession(sc)

""" 
RDD vs Dataframe performance 
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

# Test cases
# 1) Count all entries
# 2) group by machine_id then count
# 3) group by task_id and job_id then count



# Load into DF
taskUsageDf = (
    spark.read.schema(taskUsageSchema)
    .csv("../data/task_usage/*.csv.gz")
)


### Run test cases for DF

# 1) Count all entries
start = time.time()
taskUsageDf.count()
end = time.time()
print("Count all entries - DF took", end - start)


# 2) group by machine_id then count
start = time.time()
taskUsageDf.select("machine_id").groupBy("machine_id").count().collect()
end = time.time()
print("group by machine_id then count - DF took", end - start)


# 3) group by task_index and job_id then count
start = time.time()
taskUsageDf.groupBy("task_index", "job_id").count().collect()
end = time.time()
print("group by task_index and job_id then count - DF took", end - start)


# Load into RDD
taskUsageRDD = sc.textFile("../data/task_usage/*.csv.gz")
taskUsageRDD = taskUsageRDD.map(lambda x: x.split(","))

### Run test cases for RDD

# 1) Count all entries
start = time.time()
taskUsageRDD.count()
end = time.time()
print("Count all entries - RDD took", end - start)


# 2) group by machine_id then count
start = time.time()
taskUsageRDD.map(lambda x: x[4]).countByValue()
end = time.time()
print("group by machine_id then count - RDD took", end - start)


# 3) group by task_index and job_id then count
start = time.time()
taskUsageRDD.map(lambda x: (x[3], x[2])).countByValue()
end = time.time()
print("group by task_index and job_id then count", end - start)