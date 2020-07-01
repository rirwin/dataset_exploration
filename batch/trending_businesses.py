import datetime

from pyspark.sql import SparkSession
from pyspark.sql import Row

ts_format = '%Y-%m-%d %H:%M:%S'
spark = SparkSession.builder.appName("ReaderSparkBatch").getOrCreate()
sdf = spark.read.parquet('test_business_out/')

sdf.show(n=3, vertical=True)


def good_experience_count(row):
    row_dict = row.asDict()
    good_review_count_2017 = 0
    if row['REVIEW']:
        for review in row['REVIEW']:
            if int(float(review['star_rating'])) >= 4 and datetime.datetime.strptime(review['timestamp'], ts_format).year == 2017:
                good_review_count_2017 += 1
    checkin_count_2017 = 0
    if row['CHECKIN']:
        for ts in row['CHECKIN'][0]['timestamps'].split(', '):
            if datetime.datetime.strptime(ts, ts_format).year == 2017:
                checkin_count_2017 += 1
    tip_count_2017 = 0
    if row['TIP']:
        for tip in row['TIP']:
            if datetime.datetime.strptime(tip['timestamp'], ts_format).year == 2017:
                tip_count_2017 += 1
    row_dict['good_experience_count_2017'] = good_review_count_2017 + checkin_count_2017 + tip_count_2017
    row_dict['business_name'] = row['BUSINESS'][0]['name']
    row_dict['state'] = row['BUSINESS'][0]['state']
    return Row(**row_dict)

new_rdd= sdf.rdd.map(lambda row: good_experience_count(row))
new_df = spark.createDataFrame(new_rdd)
new_df.registerTempTable("df_table")
print(spark.sql("""
    SELECT
            state,
            business_id,
            business_name,
            good_experience_count_2017
    FROM
            (
                SELECT state, business_id, business_name, good_experience_count_2017, ROW_NUMBER() OVER
                (PARTITION BY state ORDER BY good_experience_count_2017 DESC) as num
                    FROM df_table
            )
    WHERE
            num <= 2
""").collect())  #[0].asDict())

new_df.show(n=1)
