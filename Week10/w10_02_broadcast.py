from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "w10_01_broadcast")
sc.setLogLevel("ERROR")

def load_boringwords():
    boring_words_set = set()
    with open("/Users/Vinoth/Desktop/Old_Files/shared1/Week10_Spark/boringwords.txt") as bw:
        for line in bw:
            line = line.strip()
            boring_words_set.add(line)
    return boring_words_set

input = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week10_Spark/bigdatacampaigndata.csv")


name_set = sc.broadcast(load_boringwords())

lines = input.map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))

flat_map = lines.flatMapValues(lambda x: x.split(" ")).map(lambda x: (x[1].lower(), x[0])).filter(lambda x: (x[0] not in name_set.value))

results = flat_map.reduceByKey(lambda x,y: (x+y)).sortBy(lambda x: (x[1]), ascending = False).collect()

for result in results:
    print(result)



