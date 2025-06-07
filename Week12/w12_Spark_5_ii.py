from pyspark.sql.types import StructType
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType


import os
import sys

from pyspark.sql.types import IntegerType, StructField, TimestampType, StringType

# 1. Simple Agg
# 2. Grouping Agg

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


my_conf = SparkConf()
my_conf.set("spark.app.name", "w12_Spark_5_ii")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf = my_conf).enableHiveSupport().getOrCreate()

invoice_df = (spark.read.
             format("csv").
             option("header", "True").
             option("inferSchema", "True").
             option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week12_Spark/order_data.csv").
             load())

# Col Object
invoice_df.select(
    count("*").alias("row_count"),
    sum(col("Quantity")).alias("tot_quantity"),
    avg(col("UnitPrice")).alias("avg_price"),
    count_distinct("InvoiceNo").alias("cnt_dist")
).show()

# Col String
invoice_df.selectExpr(
    "count(*) as row_count",
    "sum(Quantity) as tot_quantity",
    "avg(UnitPrice) as avg_price",
    "count(distinct InvoiceNo) as cnt_dist"
).show()


# Col Object
invoice_df.groupBy("Country", "InvoiceNo")\
    .agg(sum("Quantity").alias("tot_quantity"),
    sum(expr("Quantity * UnitPrice")).alias("InvoiceValue")).show()

# String Expression
invoice_df.groupBy("Country", "InvoiceNo")\
    .agg(expr("sum(Quantity) as tot_quantity"),
    expr("sum(Quantity * UnitPrice) as InvoiceValue"))\
    .show()



spark.stop()
