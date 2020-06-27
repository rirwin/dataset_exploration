from functools import reduce

import argparse
import sys
from operator import add

from pyspark.sql import SparkSession


INFILE_TO_TYPE = {
    'business': 'BUSINESS',
    'review': 'REVIEW',
    'checkin': 'CHECKIN',
    'tip': 'TIP',
}
def match_infile_to_type(infile):
    for key in INFILE_TO_TYPE.keys():
        if key in infile:
            return INFILE_TO_TYPE[key]

def process_dict(d, d_type):
    if d_type == 'BUSINESS':
        entry = {
            'business_id': d['business_id'],
            'name': d['name'],
            'city': d['city'],
            'state': d['state'],
        }
    elif d_type == 'REVIEW':
        entry = {
            'business_id': d['business_id'],
            'user_id': d['user_id'],
            'review_id': d['review_id'],
            'timestamp': d['date'],
            'star_rating': d['stars'],
            'text': d['text'],
        }
    elif d_type == 'CHECKIN':
        entry = d['date'].split(', ')
    elif d_type == 'TIP':
        entry = {
            'business_id': d['business_id'],
            'user_id': d['user_id'],
            'text': d['text'],
            'timestamp': d['date'],
        }
    return [entry]


def create_df_from_file(spark, infile):
    d_type = match_infile_to_type(infile)
    rdd = spark.read.json(infile).rdd \
        .map(lambda d: (d['business_id'], process_dict(d, d_type))) \
        .reduceByKey(lambda data1, data2: data1 + data2)
    return spark.createDataFrame(rdd, ['business_id', d_type])


def join_df_on_business_id(dfs):
    return reduce(lambda df1, df2: df1.join(df2, ['business_id'], 'left_outer'), dfs)


def parse_args(args):
	print(args)
	parser = argparse.ArgumentParser()
	parser.add_argument(
		'--infiles',
		help="input files, put the business file first",
		action='extend',
		nargs='+',
		type=str,
		required=True
	)
	args = parser.parse_args(args)
	return args


if __name__ == "__main__":

    args = parse_args(sys.argv[1:])

    spark = SparkSession.builder.appName("YelpBusinessMapper").getOrCreate()

    dfs = [create_df_from_file(spark, infile) for infile in args.infiles]
    df = join_df_on_business_id(dfs)
    df.write.json('test_write')

    spark.stop()
