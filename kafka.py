import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import SparkSession
#from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql import functions

def parseInput(line):
    fields = line.value.split()
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))

def parseInput2(line):
	fields = line.split('|')
	return Row(user_id = int(fields[0]), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[4])


"""def processRecord(records,spark):
	movieNames = {}
	fields = {}
	lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd

    # Convert it to a RDD of Row objects with (userID, movieID, rating)
	ratingsRDD = lines.map(parseInput)

    # Convert to a DataFrame and cache it
	ratings = spark.createDataFrame(ratingsRDD).cache()
#	als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
#	model = als.fit(ratings)

	for record in records.collect():
		fields = record.split('|')
		movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
		print movieNames[int(fields[0])];

	userRatings = ratings.filter("userID = 0")
	for rating in userRatings.collect():
		print movieNames[rating['movieID']], rating['rating']

	spark.stop()
"""
def processA(record):
	result = json.loads(record[1])['payload']
	print('Reaching here in process');
	movieNames = {}
	fields = {}
	print result
	fields = result.split('|')
	movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
	print(movieNames[int(fields[0])]);	
	return result
#	fields = record.split('|')
#	movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
#	print(movieNames[int(fields[0])]);

			

		
if __name__ == "__main__":		
#	sc = SparkContext(appName="PythonSparkStreamingKafka")
	spark = SparkSession.builder.appName("MovieRecs").getOrCreate()
	sc = spark.sparkContext
#	lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd
	sc.setLogLevel("WARN")
	ssc = StreamingContext(sc,2)
	print('ssc =================== {} {}');
	kafkaStream = KafkaUtils.createStream(ssc, 'sandbox.hortonworks.com:2181', 'spark-streaming', {'test':1})
#	kafkaStream.pprint()
	print('contexts =================== {} {}');
#	lines = kafkaStream.map(lambda x: json.loads(x[1])['payload'])
	lines = kafkaStream.map(processA)
#	lines.pprint()
#	lines.foreachRDD(lambda k: processRecord(k,spark))  
#	lines.foreachRDD(processA)  
	line = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd
	ratingsRDD = line.map(parseInput)
	ratings = spark.createDataFrame(ratingsRDD).cache()
#	als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
#	model = als.fit(ratings)

    # Print out ratings from user 0:
	print("\nRatings for user ID 0:")
	userRatings = ratings.filter("userID = 0")
	for rating in userRatings.collect():
		print rating['rating']
	spark = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()
	lanes = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.user")
	users = lanes.map(parseInput2)
    # Convert that to a DataFrame
	usersDataset = spark.createDataFrame(users)
	usersDataset.write\
		.format("org.apache.spark.sql.cassandra")\
		.mode('append')\
		.options(table="users", keyspace="movielens")\
		.save()
	readUsers = spark.read\
		.format("org.apache.spark.sql.cassandra")\
		.options(table="users", keyspace="movielens")\
		.load()
	readUsers.createOrReplaceTempView("users")

	sqlDF = spark.sql("SELECT * FROM users WHERE age < 20")
	sqlDF.show()
	lines.pprint()
	ssc.start()
	ssc.awaitTermination()

