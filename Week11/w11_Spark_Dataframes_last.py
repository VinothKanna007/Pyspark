from pyspark.sql.types import StructType
from pyspark import SparkConf
from pyspark.sql import SparkSession

import os
import sys

from pyspark.sql.types import IntegerType, StructField, TimestampType, StringType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Explicit Schema
# 1. Programatically
# 2. DDL

my_conf = SparkConf()
my_conf.set("spark.app.name", "w11_Spark_Dataframes_last")
my_conf.set("spark.master", "local[*]")

#C:\Users\Vinoth\Desktop\Old_Files\shared1\Week11_Spark
spark = SparkSession.builder.config(conf = my_conf).getOrCreate()

# 1. Programatically
orders_schema = StructType([
StructField("order_id", IntegerType()),
StructField("order_date", TimestampType()),
StructField("customer_id", IntegerType()),
StructField("status", StringType())
])

# 2. DDL
orders_ddl = """order_id_ddl  Integer, order_date Timestamp, customer_id  Integer, status String"""
# orders_df = (spark.read.
#              format("csv").
#              option("header", "True").
#              option("inferSchema", "True").
#              option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week11_Spark/orders.csv"))

orders_df = (spark.read.
             format("csv").
             schema(orders_ddl).
             option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week11_Spark/orders.csv").
             load())

orders_df.show()

orders_df.printSchema()

spark.stop()
