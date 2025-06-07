from pyspark.sql.types import StructType
from pyspark import SparkConf
from pyspark.sql import SparkSession

import os
import sys

from pyspark.sql.types import IntegerType, StructField, TimestampType, StringType

# 1. Spark SQL
# 2. Store in Form of Table
# 3. enableHiveSupport()
# 4. bucketBy + sortBy


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


my_conf = SparkConf()
my_conf.set("spark.app.name", "w12_Spark_2")
my_conf.set("spark.master", "local[*]")
#my_conf.set("spark.jars", "/Users/Vinoth/Downloads/spark-avro_2.12-3.5.0.jar")
#C:\Users\Vinoth\Desktop\Old_Files\shared1\Week11_Spark
spark = SparkSession.builder.config(conf = my_conf).enableHiveSupport().getOrCreate()

orders_df = (spark.read.
             format("csv").
             option("header", "True").
             option("inferSchema", "True").
             option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week11_Spark/orders.csv").
             load())
#
# orders_df.createOrReplaceTempView("orders")
#
# df_final = spark.sql("select order_status, count(*) as cnt from orders group by order_status").show()

# df_final = (orders_df.write.
#             format("csv").
#             mode("overwrite").
#             saveAsTable("orders_table"))

spark.sql("create database if not exists retails")

df_final = (orders_df.write.
            option("maxRecordsPerFile", 2000).
            format("csv").
            bucketBy(4, "order_customer_id").
            sortBy("order_customer_id").
            mode("overwrite").
            saveAsTable("retails.orders_table2"))

spark.stop()
