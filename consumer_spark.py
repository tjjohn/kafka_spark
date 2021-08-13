
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json,lit
from pyspark.sql.types import ArrayType,StructType, StructField, BooleanType, LongType, IntegerType, StringType
from datetime import date
kafka_topic_name = "coin"
kafka_bootstrap_servers = 'localhost:9092'


mysql_host_name = "localhost"
mysql_port_no = "3306"
mysql_user_name = "root"
mysql_password = ""
mysql_database_name = "spark"
mysql_driver_class = "com.mysql.jdbc.Driver"
mysql_table_name = "crypto_spark_table"


#Create the Database properties
db_properties = {}
db_properties['user'] = mysql_user_name
db_properties['password'] = mysql_password
db_properties['driver'] = mysql_driver_class

def save_to_mysql_table(current_df, epoc_id):
    print("Inside save_to_mysql_table function")
    print("Printing epoc_id: ")
    print(epoc_id)
    mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + str(
    mysql_port_no) + "/" + mysql_database_name + "?autoReconnect=true&useSSL=false"
    print(mysql_jdbc_url)

    current_df=current_df.withColumn("batchid", lit(epoc_id))

    try:

        #Save the dataframe to the table.
        current_df.write.jdbc(url = mysql_jdbc_url,
                    table = mysql_table_name,
                    mode = 'append',
                    properties = db_properties)
    except :
        print("connection eroor")
    print("Exit out of save_to_mysql_table function")


if __name__ == "__main__":
    print(" Data Processing  Started ...")
    # the entry point to Spark SQL  and also has spark configuration like spark,sparksqlkafka dependencies and mysqlconnector dependarncy
    # and extra config to run on cluster
    spark = SparkSession \
        .builder \
        .appName(" Spark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .config("spark.jars", "file:///C://spark_dependency_jars//commons-pool2-2.8.1.jar,file:///C://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar,file:///C://spark_dependency_jars//kafka-clients-2.6.0.jar,file:///C://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar,file:///C://spark_dependency_jars//mysql-connector-java-5.1.46.jar") \
        .config("spark.executor.extraClassPath","file:///C://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///C://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///C://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///C://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .config("spark.executor.extraLibrary","file:///C://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///C://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///C://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///C://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .config("spark.driver.extraClassPath", "file:///C://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///C://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///C://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///C://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .getOrCreate()
    #spark context and  turn offed spark Info logging
    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from topic ie input
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()


    #https://databricks.com/blog/2017/02/23/working-complex-data-formats-structured-streaming-apache-spark-2-1.html

    # starting offset -> latest messages
    #got the dstream
    print("schema of dstream")
    df.printSchema()  # schema of dataframe consists of key, value,offset,topic  where  value is the actual message

    #extraact only message & change its datatype
    df1 = df.selectExpr( "CAST(value AS string)"  )
    print("\n datatype of extracted message is ",type(df1))
    print("schema is ")
    df1.printSchema()


    # defineing a schema based on json we about get
    schema = ArrayType(StructType([
        StructField("currancy", StringType()),
         StructField("volume", StringType())
         ]) )


    #changing column name of value and integrating with schema
    df2 = df1.select(from_json(col("value"), schema).alias("new_value"))
    print("\n schema of dataframe after combining manual StructType")

    df2.printSchema()


    # explode() can be used to create a new row for each element in an array or each key-value pair

    df3=df2.select(explode("new_value").alias("new_value1"))

    df3.printSchema()



    #flatten the json structure or it automactically flatten structre
    df4 = df3.select(["new_value1.currancy", \
      "new_value1.volume"])

    df4=df4.withColumn("Volume", regexp_replace("volume", "BTC", ""))
    df4 = df4.withColumn("Volume", regexp_replace("Volume", " ", ""))

    df4 = df4.withColumnRenamed("Currancy", "currancy")



    df4 = df4.selectExpr("CAST(Volume AS Float)","CAST(Currancy AS string)")

    #df4=df4.filter(df4["Volume"]>50)

    df4=df4.withColumn("batchid",lit(1))

    df4.createOrReplaceTempView("Temptable")

    df5=spark.sql("select  Currancy,max(Volume) as Max_Volume,batchid from Temptable group by Currancy,batchid having Max_Volume>100  ")

    # Write result  into console for debugging
    # stream1=df5 \
    #     .writeStream \
    #     .trigger(processingTime='5 seconds') \
    #     .outputMode("update") \
    #     .option("truncate", "false")\
    #     .format("console") \
    #     .start()


    #Watermarking is to handle late data:   so add timestamp column
    #csv sinking
    # stream1=df5 \
    #     .writeStream\
    #     .outputMode("append")\
    #     .trigger(processingTime="10 seconds") \
    #     .format("csv") \
    #     .option("checkpointLocation", "checkpoint") \
    #     .option("header", True)\
    #     .option("path", "checkpoint/filesink_checkpoint")\
    #     .start()
        #check point contains the state of our app E.g. the sources folder contains batches of data processed


    stream1=df5 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .foreachBatch(lambda current_df, epoc_id: save_to_mysql_table(current_df, epoc_id)) \
        .start()
    #for each batch write to mysql
    #epocid is default for foreachbatch


    stream1.awaitTermination()

    print("Stream Data Processing Application Completed.")