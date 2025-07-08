from pyspark import SparkContext
from pyspark import StorageLevel
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Cache and persist StorageLevels

sc = SparkContext("local[*]", "w11_Spark_in_Depth_7 ")
sc.setLogLevel("ERROR")
input = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week9_Spark/customerorders.csv")

rdd2 = input.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))

final = rdd2.reduceByKey(lambda x, y: (x + y)).filter(lambda x: x[1]>5000).map(lambda x: (x[0], x[1]*2)).persist(StorageLevel.MEMORY_ONLY)

print("Total cnt" + str(final.count()))

results = final.collect()

for result in results:
    print(result)

sys.stdin.readline()
