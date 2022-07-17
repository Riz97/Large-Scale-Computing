from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

spark = SparkSession \
    .builder \
    .appName("Music Project") \
    .getOrCreate()


listensPath = "hdfs:/user/user_lsc_3/listenings.csv"
networksPath = "hdfs:/user/user_lsc_3/network.csv"
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Started on =" , current_time)

listensDF = spark.read.csv(listensPath, header = True, inferSchema = True)
#listensDF.printSchema()
networksDF = spark.read.csv(networksPath, header = True, inferSchema = True)

print("All songs listened by a user and one of his/her friend")
listensDF.createOrReplaceTempView("listenings")
networksDF.createOrReplaceTempView("networks")

tracks = spark.sql("SELECT DISTINCT track FROM listenings JOIN networks ON user_id=user_id1 WHERE user_id2 IN (SELECT user_id FROM listenings L WHERE L.track=track)")

track_names = tracks.rdd.map(lambda p : p.track).collect()
for name in track_names:
    print(name)

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Ended on =", current_time)
spark.stop()
