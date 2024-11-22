from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


spark = SparkSession \
    .builder \
    .appName("StructuredStreamWordCount") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9998) \
    .load()

# Split the lines into words
words = lines.select( \
   explode( \
       split(lines.value, " ") \
   ).alias("word") \
)

# Generate running word count
wordCounts = words.groupBy("word").count()

query = wordCounts \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()