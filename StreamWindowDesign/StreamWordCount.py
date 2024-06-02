from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Let us split the strings based on spaces and explode the list to create words column
words_df = lines.selectExpr("explode(split(value, ' ')) as word")

# Check the schema
words_df.printSchema()
