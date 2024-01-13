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
my_conf.set("spark.app.name", "w12_Spark_4")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf = my_conf).enableHiveSupport().getOrCreate()

def age_check(age):
    if(age>18):
        return 'Y'
    else:
        return 'N'


people_df = (spark.read.
             format("csv").
             option("inferSchema", "True").
             option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week12_Spark/dataset1").
             load())

df2 = people_df.toDF("name", "age", "city")

#1. Col object UDF -> wont register in spark catalog

age_fn = udf(age_check,StringType())

df2.withColumn("adult", age_fn(col("age"))).show()

#2. SQL/String UDF -> Will register in spark catalog

spark.udf.register("age_register", age_check,StringType())

for x in spark.catalog.listFunctions():
    print(x)

df2.withColumn("adult", expr("age_register(age)")).show()

spark.stop()
