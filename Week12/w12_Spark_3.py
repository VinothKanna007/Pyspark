from pyspark.sql.types import StructType
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType


import os
import sys

from pyspark.sql.types import IntegerType, StructField, TimestampType, StringType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


my_conf = SparkConf()
my_conf.set("spark.app.name", "w12_Spark_3")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf = my_conf).enableHiveSupport().getOrCreate()

my_reg = r'^(\S+) (\S+)\t(\S+)\,(\S+)'

# Dealing with Unstructured Files
#
# lines_df = spark.read.text("/Users/Vinoth/Desktop/Old_Files/shared1/Week12_Spark/orders_new.csv")
#
# final_df = lines_df.select(regexp_extract('value', my_reg,1).alias("order_id"),
#                 regexp_extract('value', my_reg,2).alias("order_date"),
#                 regexp_extract('value', my_reg,3).alias("customer_id"),
#                 regexp_extract('value', my_reg,4).alias("status"))
#
# changedTypedf = (final_df.
#                  withColumn("order_id", final_df["order_id"].cast(IntegerType())).
#                  withColumn("order_date", final_df["order_date"].cast(TimestampType())).
#                  withColumn("customer_id", final_df["customer_id"].cast(IntegerType())).
#                  withColumn("status", final_df["status"].cast(StringType())))
#
# changedTypedf.printSchema()
#
# changedTypedf.show()


orders_df = (spark.read.
             format("csv").
             option("header", "True").
             option("inferSchema", "True").
             option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week12_Spark/orders.csv").
             load())

# Col String
orders_df.select("order_id", "order_date").show()

# Col Object
orders_df.select(col("order_id"), column("order_date")).show()

# Expression will convert into col object
orders_df.selectExpr("order_id", "concat(order_status,'_STATUS') as con").show()

spark.stop()
