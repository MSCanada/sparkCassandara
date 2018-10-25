/usr/hdp/current/kafka-broker/bin

./connect-standalone.sh ~/connect-standalone.properties ~/connect-file-source.properties

export SPARK_MAJOR_VERSION=2

 spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 /kafka.py
 
 echo "My name is khan" >> /home/maria_dev/sample.txt
 
 
 optional: scp -P 2222 sample.txt root@127.0.0.1:/home/maria_dev/

Data Ingestion Platform: Real time Data ingetsion In Cassandra through Spark from Kafka
Tools:: Cassandra, Spark, Kafka

Reference :: The Ultimate Hands-On Hadoop Udemy Course from Frank kane Sundog
