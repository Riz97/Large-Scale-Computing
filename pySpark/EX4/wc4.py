# create a program wc4.py to select all words starting and ending 
# with a vowel which occur in the dataset for more than the average 
# number computed in (3)

import re
 
from pyspark import SparkContext

_DATA_ = "hdfs:/user/user_lsc_3/labPySparkData/big.txt"
_OUTPUT_ = "hdfs:/user/user_lsc_3/labPySparkData/ex4"

def StartsEndsWithVowel(x):
   return re.match(r"\b[aeiou][a-zA-Z]*[aeiou]\b", x)

def HasHigherThanAverageCount(x, avg):
    return x > avg

def myFilter(x, avg):
    return StartsEndsWithVowel(x[1]) and HasHigherThanAverageCount(x[0],avg)

if __name__ == "__main__":
	
    # create Spark context with necessary configuration
    
    sc = SparkContext("local","PySpark Word Count Exmaple")
    
    # read data from text file and split each line into words
    rdd = sc.textFile(_DATA_).flatMap(lambda line: re.split(r"[^\w]*", line.strip().lower()))
 
    # the counts of ords
    wordCounts = rdd.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a + b)

    # some numbers
    total_number_of_words = wordCounts.count()   
    total_word_count = wordCounts.map(lambda x: x[1]).sum()
    
    # average
    avg = total_word_count / total_number_of_words
    

    counts = wordCounts.map(lambda x: (x[1], x[0]))
    
    # Filter - sort
    vowelcounts = sc.parallelize(counts.filter(lambda x: myFilter(x,avg)).collect())
    
    vowelcounts.saveAsTextFile(_OUTPUT_)   