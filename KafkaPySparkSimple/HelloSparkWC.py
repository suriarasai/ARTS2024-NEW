# Create SparkSession
from pyspark.sql import SparkSession
#import required pckg
from pyspark.sql.functions import explode,split,col

spark = SparkSession.builder\
 .master("local")\
 .appName('word_count')\
 .getOrCreate()
# Read the input file and Calculating words count
text_file = spark.read.text("random.txt")
#Apply Split, Explode and groupBy to get count()
df_count=(
  text_file.withColumn('word', explode(split(col('value'), ' ')))
    .groupBy('word')
    .count()
    .sort('count', ascending=False)
)

#Display Output
print(df_count.count())
'''

counts = text_file.flatMap(lambda line: line.split(" ")) \
                            .map(lambda word: (word, 1)) \
                           .reduceByKey(lambda x, y: x + y)
output = counts.collect()
'''