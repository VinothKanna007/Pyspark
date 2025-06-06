from pyspark import SparkContext
import os
import sys


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# parallelize - For Experimentation

sc = SparkContext("local[*]", "w10_Spark_in_Depth_1")
sc.setLogLevel("ERROR")


input = ["ERROR: Thu Jun 04 10:37:51 BST 2015",
"WARN: Sun Nov 06 10:37:51 GMT 2016",
"WARN: Mon Aug 29 10:37:51 BST 2016",
"ERROR: Thu Dec 10 10:37:51 GMT 2015",
"ERROR: Fri Dec 26 10:37:51 GMT 2014",
"ERROR: Thu Feb 02 10:37:51 GMT 2017",
"WARN: Fri Oct 17 10:37:51 BST 2014",
"ERROR: Wed Jul 01 10:37:51 BST 2015",
"WARN: Thu Jul 27 10:37:51 BST 2017",
"WARN: Thu Oct 19 10:37:51 BST 2017"]

base_rdd = sc.parallelize(input)

rdd2 = base_rdd.map(lambda x: (x.split(":")[0],1))

results = rdd2.reduceByKey(lambda x, y: (x+y)).collect()

for result in results:
    print(result)
