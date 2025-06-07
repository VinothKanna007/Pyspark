from pyspark.sql.types import StructType
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType


import os
import sys

from pyspark.sql.types import IntegerType, StructField, TimestampType, StringType

# Usecase
# Agg & pivot

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


my_conf = SparkConf()
my_conf.set("spark.app.name", "w12_Spark_5_v")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf = my_conf).enableHiveSupport().getOrCreate()

logs_df = (spark.read.
             format("csv").
             option("header", "True").
             option("inferSchema", "True").
             option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week12_Spark/biglog.txt").
             load())

logs_df.createOrReplaceTempView("log")


# new_log = spark.sql("""select level,
#              date_format(datetime, 'MMMM') as month,
#              cast(first(date_format(datetime, 'MM'))as int) as month_num,
#              count(1) total from log
#              group by level, date_format(datetime, 'MMMM')
#              order by month_num""").show(100)
columns = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

final_df = spark.sql("""select level,
             date_format(datetime, 'MMMM') as month,
             cast(first(date_format(datetime, 'MM'))as int) as month_num
             from log""").groupBy("level").pivot("month", columns).count()\
.withColumn("Total",expr("January+ February+ March+ April+ May+ June+ July+ August+ September+ October+ November+ December"))

union_df = final_df.selectExpr("'Total'", "sum(January)", "sum(February)", "sum(March)", "sum(April)", "sum(May)", "sum(June)",
 "sum(July)", "sum(August)", "sum(September)", "sum(October)", "sum(November)", "sum(December)", "sum(Total)")

final_df.unionAll(union_df).show()

sys.stdin.readline()

spark.stop()

