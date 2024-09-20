from pyspark.sql import SparkSession

from pyspark.sql.types import StructType

spark = SparkSession \
    .builder \
    .appName("StructuredStreamFromCSV") \
    .getOrCreate()



# Read all the csv files written atomically in a directory
userSchema = StructType().add("name", "string").add("age", "integer")
csvDF = spark \
    .readStream \
    .option("sep", ",") \
    .option("header", "true") \
    .schema(userSchema) \
    .csv("./data")  # Equivalent to format("csv").load("/path/to/directory")
query = csvDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

