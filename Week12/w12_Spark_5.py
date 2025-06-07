from pyspark.sql.types import StructType
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType


import os
import sys

from pyspark.sql.types import IntegerType, StructField, TimestampType, StringType

# 1. List to DF
# 2. Column Names using toDF()
# 3. monotonically_increasing_id
# 4. Convert to Timestamp  https://www.unixtimestamp.com/
# 5. dropDuplicates
# 6. drop
# 7. sort

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


my_conf = SparkConf()
my_conf.set("spark.app.name", "w12_Spark_5")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf = my_conf).enableHiveSupport().getOrCreate()


my_list =  [(1,"2013-07-25",11599,"CLOSED"),
    (2,"2013-07-25",256,"PENDING_PAYMENT"),
    (3,"2013-07-25",12111,"COMPLETE"),
    (4,"2013-07-25",8827,"CLOSED"),
    (5,"2013-07-25",11318,"COMPLETE")]


orders_df = (spark.createDataFrame(my_list).
             toDF("order_id","order_date", "customer_id", "status"))

orders_df.withColumn("date", unix_timestamp(col("order_date").cast(TimestampType())))\
.withColumn("new_id", monotonically_increasing_id())\
.dropDuplicates(["order_date", "customer_id"])\
.drop("order_id")\
.sort("order_date").show()


spark.stop()
