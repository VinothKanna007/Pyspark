from pyspark import SparkContext
import os
import sys


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "w10_03_accumulators")
sc.setLogLevel("ERROR")

def accumulate(input):
    if(input == ""):
        acum.add(1)

input = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week10_Spark/samplefile.txt")

acum = sc.accumulator(0)

input.foreach(accumulate)

print(acum.value)

sys.stdin.readline()
