
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import ArrayType,StructType, StructField, BooleanType, LongType, IntegerType, StringType


kafka_topic_name = "coin"
kafka_bootstrap_servers = 'localhost:9092'
if __name__ == "__main__":
    print(" Data Processing  Started ...")
    # session for specfic users
    spark = SparkSession \
        .builder \
        .appName(" Spark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .config("spark.jars", "file:///C://spark_dependency_jars//commons-pool2-2.8.1.jar,file:///C://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar,file:///C://spark_dependency_jars//kafka-clients-2.6.0.jar,file:///C://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .config("spark.executor.extraClassPath","file:///C://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///C://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///C://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///C://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .config("spark.executor.extraLibrary","file:///C://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///C://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///C://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///C://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .config("spark.driver.extraClassPath", "file:///C://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///C://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///C://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///C://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .getOrCreate()

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
    df1 = df.selectExpr( "CAST(value AS string)")
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



    df4.createOrReplaceTempView("Temptable")

    df5=spark.sql("select  Currancy,max(Volume) as Max_Volume from Temptable group by Currancy having Max_Volume>100  ")

    #sinking
    stream1=df5 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()
    # Note that you have to call start() to actually start the execution of the query
    # trigger interval set to 5 seconds(ie like batch interval in spark streaming)
    # staging
    stream1.awaitTermination()

    print("Stream Data Processing Application Completed.")