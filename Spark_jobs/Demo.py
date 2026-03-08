from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("demo").getOrCreate()

df = spark.range(5)
df.show()

spark.stop()