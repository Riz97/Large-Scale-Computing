from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

spark = SparkSession \
    .builder \
    .appName("Music Project") \
    .getOrCreate()


listensPath = "hdfs:/user/user_lsc_3/listenings.csv"
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Started on =" , current_time)

listensDF = spark.read.csv(listensPath,header = True, inferSchema = True)
#listensDF.printSchema()

print("List of Users who listened to more than 100 songs in 2016")
listensDF.createOrReplaceTempView("listenings")
users = spark.sql("SELECT user_id, COUNT(track)  FROM listenings WHERE date BETWEEN 1451606461  AND 1483228799   GROUP BY user_id HAVING COUNT(track)>1 ")


usernames = users.rdd.map(lambda p : p.user_id).collect()
for name in usernames:
    print(name)

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Ended on =", current_time)
spark.stop()