from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


spark = SparkSession \
    .builder \
    .appName("SimpleStructuredStream") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split the lines into words
words = lines.select( \
   explode( \
       split(lines.value, " ") \
   ).alias("word") \
)

query = words \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()