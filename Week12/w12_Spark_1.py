from pyspark.sql.types import StructType
from pyspark import SparkConf
from pyspark.sql import SparkSession

import os
import sys

from pyspark.sql.types import IntegerType, StructField, TimestampType, StringType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


my_conf = SparkConf()
my_conf.set("spark.app.name", "w12_Spark_1")
my_conf.set("spark.master", "local[*]")
my_conf.set("spark.jars", "/Users/Vinoth/Downloads/spark-avro_2.12-3.5.0.jar")
#C:\Users\Vinoth\Desktop\Old_Files\shared1\Week11_Spark
spark = SparkSession.builder.config(conf = my_conf).getOrCreate()


orders_df = (spark.read.
             format("csv").
             option("header", "True").
             option("inferSchema", "True").
             option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week11_Spark/orders.csv").
             load())

print("Number of Partitions ", orders_df.rdd.getNumPartitions())

df_re = orders_df.repartition(4)

print("Number of Partitions ", df_re.rdd.getNumPartitions())

df_final = (df_re.write.
            format("avro").
            partitionBy("order_status").
            mode("overwrite").
            option("path", "C:/Users/Vinoth/Desktop/folder1").
            save())

spark.stop()
