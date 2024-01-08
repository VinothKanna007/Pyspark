from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Usecase 1 -  Find Top 10 customers who spent max amount

sc = SparkContext("local[*]", "Spark practical 6")
sc.setLogLevel("ERROR")

def parse_line(input):
    fields = input.split("::")
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

friends_data = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week9_Spark/friendsdata.csv")


rdd2 = friends_data.map(parse_line)

rdd3 = rdd2.mapValues(lambda x: (x,1))

rdd4 = rdd3.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

results = rdd4.mapValues(lambda x: (x[0]/x[1])).collect()

for result in results:
    print(result)