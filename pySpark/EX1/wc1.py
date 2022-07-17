# create a program wc1.py with a map-reduce scheme to select the 
# 3 words that occur more frequently in the dataset

import sys
import re
 
from pyspark import SparkContext

_DATA_ = "hdfs:/user/user_lsc_3/labPySparkData/big.txt"
_OUTPUT_ = "hdfs:/user/user_lsc_3/labPySparkData/ex1"



if __name__ == "__main__":
	
    # create Spark context with necessary configuration
    
    sc = SparkContext("local","PySpark Word Count Exmaple")
    
    # read data from text file and split each line into words
    rdd = sc.textFile(_DATA_).flatMap(lambda line: re.split(r"[^\w]*", line.strip().lower()))
    
    wordCounts = rdd.map(lambda word: (word.lower(), 1)).reduceByKey(lambda a,b:a +b)
    
    top3 = sc.parallelize(wordCounts.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).filter(lambda x: x!='').take(3))
    
    top3.saveAsTextFile(_OUTPUT_)
        