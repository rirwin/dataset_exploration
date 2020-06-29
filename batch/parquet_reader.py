from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ReaderSparkBatch").getOrCreate()
df = spark.read.parquet('test_write/')
df.show(n=3, vertical=True)
print(df.count(), 'records found')
