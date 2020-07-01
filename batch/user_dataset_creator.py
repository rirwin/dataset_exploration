from functools import reduce
from typing import Dict, List

import argparse
import sys
from operator import add

from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import SparkSession  # type: ignore


INFILE_TO_TYPE = {
    'user': 'USER',
    'review': 'REVIEW',
    'tip': 'TIP',
}


class UserDatasetCreatorSparkBatch(object):
    def match_infile_to_type(self, infile: str) -> str:
        for key in INFILE_TO_TYPE.keys():
            if key in infile:
                return INFILE_TO_TYPE[key]
        raise Exception('Invalid input file')

    def process_dict(self, d: Dict[str, str], d_type: str) -> List:
        if d_type == 'USER':
            entry = {
                'user_id': d['user_id'],
                'name': d['name'],
                'review_count': d['review_count'],
                'average_rating': d['average_stars'],
            }
        elif d_type == 'REVIEW':
            entry = {
                'user_id': d['user_id'],
                'business_id': d['business_id'],
                'review_id': d['review_id'],
                'timestamp': d['date'],
                'star_rating': d['stars'],
                'text': d['text'],
            }
        elif d_type == 'TIP':
            entry = {
                'user_id': d['user_id'],
                'business_id': d['business_id'],
                'text': d['text'],
                'timestamp': d['date'],
            }
        return [entry]

    def create_df_from_file(self, spark: SparkSession, infile: str) -> DataFrame:
        d_type = self.match_infile_to_type(infile)
        rdd = spark.read.json(infile).rdd \
            .map(lambda d: (d['user_id'], self.process_dict(d, d_type))) \
            .reduceByKey(lambda data1, data2: data1 + data2)
        return spark.createDataFrame(rdd, ['user_id', d_type])

    def join_df_on_user_id(self, dfs: List[DataFrame]) -> DataFrame:
        return reduce(lambda df1, df2: df1.join(df2, ['user_id'], 'left_outer'), dfs)

    def parse_args(self, sys_args: List[str]) -> argparse.Namespace:
        parser = argparse.ArgumentParser()
        parser.add_argument(
    	    '--infiles',
	    	help="input files, put the user file first",
    		action='extend',
    		nargs='+',
    		type=str,
    		required=True
    	)
        parser.add_argument(
    	    '--outfiles',
	    	help="output files directory/path/location",
    		required=True,
    	)
        return parser.parse_args(sys_args)

    def run(self):
        args = self.parse_args(sys.argv[1:])

        spark = SparkSession.builder.appName("UserDatasetCreatorSparkBatch").getOrCreate()

        dfs = [self.create_df_from_file(spark, infile) for infile in args.infiles]
        df = self.join_df_on_user_id(dfs)
        df.write.parquet(args.outfiles)

        spark.stop()


if __name__ == "__main__":
    UserDatasetCreatorSparkBatch().run()
