
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, BooleanType, LongType, IntegerType, StringType

kafka_topic_name = "coin"
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
    print(" Data Processing  Started ...")
    # session for specfic users
    spark = SparkSession \
        .builder \
        .appName("Spark Structured Streaming with Kafka and Message Format as JSON") \
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
    # starting offset -> latest messages
    #got the dstream
    print("schema of dstream")
    df.printSchema()  # schema of dataframe consists of key, value,offset,topic  where  value is the actual message
    # defineing a schema based on json we about get
    schema = StructType([
        StructField("rsvp_id", IntegerType()),
        StructField("mtime", IntegerType()),
        StructField("guests", IntegerType())
    ])



    #change datatype of dataframe
    df1 = df.selectExpr( "CAST(value AS string)","CAST(timestamp AS TIMESTAMP)")
    #changing column name of value and integrating with schema
    df2 = df1.select(from_json(col("value"), schema).alias("new_value"))
    print("\n schema of dataframe after taking VALUE from dstream")
    df2.printSchema()

    #getting all json message by columwise.....inner structure  not displayed..so if needed to split that column(important)
    df3 = df2.select("new_value.*")

    #flatten the json structure
    df4 = df3.select(["rsvp_id", \
    "mtime", \
    "guests"])

    #type is column
    print("\ntype of  df['guests']",df4['guests'])

    ######operations#######
    #select
    #df4 = df4.select("guests").where("guests > 1")

    #filtering
    #df4=df4.filter(df4["guests"]>0)


    #groupby
    #df4=df4.groupBy("rsvp_id").agg(fn.sum('guests').alias('total_guests'))
    # df4=df4.groupBy("rsvp_id").count()

    #temporary view and then apply SQL commands
    df4.createOrReplaceTempView("Temptable")
    df5=spark.sql("select * from Temptable")  ## returns another streaming DF

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