import sys
 
from pyspark import SparkContext, SparkConf
 
if __name__ == "__main__":
	
	# create Spark context with necessary configuration
	sc = SparkContext("local","PySpark Word Count Exmaple")
	        
	# read data from text file and split each line into words
	words = sc.textFile("hdfs:/user/user_lsc_3/labPySparkData/big.txt").flatMap(lambda line: line.split(" ")).take(2)
	
	for w in words:
		print(w,"\n")
