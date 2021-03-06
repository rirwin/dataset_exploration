import argparse
import datetime
import sys
from typing import List

from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql import Row

ts_format = '%Y-%m-%d %H:%M:%S'


class BusinessStructuredDataSparkBatch():

    args = None

    def run(self):
        spark = SparkSession.builder.appName("BusinessStructuredDataSparkBatch").getOrCreate()
        self.parse_args(sys.argv[1:])
        sdf = spark.read.parquet(self.args.infiles)
        rdd_with_derived_data = sdf.rdd.map(lambda row: self.add_derived_columns(row))
        new_df = spark.createDataFrame(rdd_with_derived_data)
        new_df.show(n=1, vertical=True)
        new_df.write.parquet(self.args.outfiles)

    def parse_args(self, sys_args: List[str]):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--infiles',
            help="input directory of business dataset parquet files",
            required=True
        )
        parser.add_argument(
            '--outfiles',
            help="output files directory/path/location",
            required=True,
        )
        self.args = parser.parse_args(sys_args)

    def add_derived_columns(self, row: Row) -> Row:
        row_dict = row.asDict()
        num_reviews = 0
        good_review_count = 0
        if row['REVIEW']:
            num_reviews = len(row['REVIEW'])
            for review in row['REVIEW']:
                if int(float(review['star_rating'])) >= 4:
                    good_review_count += 1
        num_checkins = 0
        if row['CHECKIN']:
            num_checkins = len(row['CHECKIN'][0]['timestamps'].split(', '))
        num_tips = 0
        if row['TIP']:
            num_tips = len(row['TIP'])
        row_dict['num_checkins'] = num_checkins
        row_dict['num_tips'] = num_tips
        row_dict['num_reviews'] = num_reviews
        row_dict['good_review_count'] = good_review_count
        row_dict['business_name'] = row['BUSINESS'][0]['name']
        row_dict['state'] = row['BUSINESS'][0]['state']
        row_dict['city'] = row['BUSINESS'][0]['city']
        row_dict['is_rfn'] = any(
            c for c in row['BUSINESS'][0]['categories'].split(',') if
            c.lower().strip() in ('restaurants', 'food', 'nightlife')
        )
        # Testing removing columns to see how this df and source df joins
        for col in ['BUSINESS', 'REVIEW', 'TIP', 'CHECKIN']:
            row_dict.pop(col)
        return Row(**row_dict)


if __name__ == '__main__':
    BusinessStructuredDataSparkBatch().run()
