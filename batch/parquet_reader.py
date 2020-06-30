from pyspark.sql import SparkSession
import sys
spark = SparkSession.builder.appName("ReaderSparkBatch").getOrCreate()
df = spark.read.parquet(sys.argv[1])
df.show(n=3, vertical=True)
print(df.count(), 'records found')