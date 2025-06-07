from pyspark.sql.types import StructType
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType


import os
import sys

from pyspark.sql.types import IntegerType, StructField, TimestampType, StringType

# Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import yaml
import os

print(os.getcwd())
with open("../conf/config.yml", "r") as stream:
    try:
        conf = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

tot = int(conf["filter"]["cust_id"])
print(tot)
print(f'tot_quantity = {tot}')

my_conf = SparkConf()
my_conf.set("spark.app.name", "w12_Spark_5_iii")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf = my_conf).enableHiveSupport().getOrCreate()

invoice_df = (spark.read.
             format("csv").
             option("inferSchema", "True").
             option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week12_Spark/windowdata.csv").
             load())

invoice_df2 = invoice_df.toDF("Country", "weeknum", "numinvoices", "tot_quantity", "Invoicevalue")

# My Window
my_window = Window.partitionBy("Country").orderBy("weeknum")\  # orderBy(desc("weeknum"))
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

my_df = invoice_df2.withColumn("run_sum", sum("Invoicevalue")\
        .over(my_window)).where(f'tot_quantity = {tot}')

my_df.printSchema()

my_df.show()

spark.stop()

