from pyspark.sql import SparkSession  # type: ignore
import sys
spark = SparkSession.builder.appName("ReaderSparkBatch").getOrCreate()

df = spark.read.parquet(sys.argv[1])
df.show(n=1, vertical=True)
print(df.count(), 'records found')
print(df.schema)
