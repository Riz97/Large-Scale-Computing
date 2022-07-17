# create a program wc2.py with a map-reduce scheme to select the 3 
# words starting and ending with a vowel that occur more frequently in the dataset

import sys
 
import re
 
from pyspark import SparkContext, SparkConf

_DATA_ = "hdfs:/user/user_lsc_3/labPySparkData/big.txt"
_OUTPUT_ = "hdfs:/user/user_lsc_3/labPySparkData/output"


def StartsEndsWithVowel(x):
   return re.match(r"\b[aeiou][a-zA-Z]*[aeiou]\b", x)
   # return x[0] in _VOWELS_ and x[-1] in _VOWELS_


if __name__ == "__main__":
    sc = SparkContext("local","PySpark Word Count Exmaple")
    rdd = sc.textFile(_DATA_).flatMap(lambda line: re.split(r"[^\w]*", line.strip().lower()))
    
    # Contatore parola
    counts = rdd.map(lambda word: (word.lower(), 1)).reduceByKey(lambda a,b:a +b).map(lambda x: (x[1], x[0]))
    
    # Filter - sort
    vowelcounts = sc.parallelize(counts.filter(lambda x: StartsEndsWithVowel(x[1])).sortByKey(ascending=False).take(3))
    
    # save text into hadoop file system
    vowelcounts.saveAsTextFile(_OUTPUT_)
                 
