from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "w9_assignment_2")
sc.setLogLevel("ERROR")

def parse_lines(input):
    fields = input.split(",")
    station_id = fields[0]
    entry_type = fields[2]
    temperature = fields[3]
    return (station_id, entry_type, temperature)

temperature_data = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week9_Spark/Assignment/temp_data.csv")

rdd2 = temperature_data.map(parse_lines)

rdd3 = rdd2.filter(lambda x: x[1] == 'TMIN')
rdd4 = rdd3.map(lambda x: (x[0], float(x[2])))
rdd5 = rdd4.reduceByKey(lambda x,y: min(x,y))

print(rdd5.collect())
