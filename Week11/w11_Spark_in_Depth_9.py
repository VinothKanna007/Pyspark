from pyspark import SparkContext
from pyspark import StorageLevel
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Usecase 1 -  Find Top 10 customers who spent max amount

sc = SparkContext("local[*]", "w11_Spark_in_Depth_9 ")
sc.setLogLevel("ERROR")
movies_base_rdd = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week11_Spark/movies.dat")
ratings_base_rdd = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week11_Spark/ratings.dat")

rating_rdd = ratings_base_rdd.map(lambda x: (int(x.split("::")[1]), int(x.split("::")[2])))

movie_rdd = movies_base_rdd.map(lambda x: (int(x.split("::")[0]), x.split("::")[1]))

reducer = rating_rdd.mapValues(lambda x: (float(x),1.0)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

fiter_rdd = reducer.filter(lambda x: x[1][1]>1000).mapValues(lambda x: (x[0]/x[1])).filter(lambda x: x[1]>4.5)

join_rdd = fiter_rdd.join(movie_rdd).sortBy(lambda x: x[1][0], ascending=False).map(lambda x: (x[1][1])).collect()

for join_rdds in join_rdd:
    print(join_rdds)
