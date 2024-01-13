from pyspark.sql.types import StructType
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType


import os
import sys

from pyspark.sql.types import IntegerType, StructField, TimestampType, StringType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


my_conf = SparkConf()
my_conf.set("spark.app.name", "w12_Spark_5_iv")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf = my_conf).enableHiveSupport().getOrCreate()

orders_df = (spark.read.
             format("csv").
             option("header", "True").
             option("inferSchema", "True").
             option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week12_Spark/orders.csv").
             load())

customers_df = (spark.read.
             format("csv").
             option("header", "True").
             option("inferSchema", "True").
             option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week12_Spark/customers.csv").
             load())

join_condition = orders_df["order_customer_id"] == customers_df["customer_id"]

# spark.sql("set spark.sql.autoBroadcastJoinThreshold =-1")

join_df = orders_df.join(broadcast(customers_df),join_condition, 'inner').show()

sys.stdin.readline()

spark.stop()

