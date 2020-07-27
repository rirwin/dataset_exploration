from functools import reduce
from typing import Dict, List

import argparse
import sys
from operator import add

from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import SparkSession  # type: ignore


INFILE_TO_TYPE = {
    'business': 'BUSINESS',
    'checkin': 'CHECKIN',
    'review': 'REVIEW',
    'user': 'USER',
    'tip': 'TIP',
}


class BaseDatasetCreatorSparkBatch(object):

    args = None
    key_column = None

    def process_dict(self, d: Dict[str, str], d_type: str) -> List:
        raise NotImplementedError

    def match_infile_to_type(self, infile: str) -> str:
        for key in INFILE_TO_TYPE.keys():
            if key in infile:
                return INFILE_TO_TYPE[key]
        raise Exception('Invalid input file')

    def create_df_from_file(self, spark: SparkSession, infile: str) -> DataFrame:
        d_type = self.match_infile_to_type(infile)
        rdd = spark.read.json(infile).rdd \
            .map(lambda d: (d[self.key_column], self.process_dict(d, d_type))) \
            .reduceByKey(lambda data1, data2: data1 + data2)
        return spark.createDataFrame(rdd, [self.key_column, d_type])

    def join_df_on(self, dfs: List[DataFrame], join_column: str) -> DataFrame:
        return reduce(lambda df1, df2: df1.join(df2, [join_column], 'left_outer'), dfs)

    def parse_args(self, sys_args: List[str]):
        parser = argparse.ArgumentParser()
        parser.add_argument(
    	    '--infiles',
	    	help="input files, put the key column file first",
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
        self.args = parser.parse_args(sys_args)

    def get_spark_session(self) -> SparkSession:
        return SparkSession.builder.appName(self.__class__.__name__).getOrCreate()

    def run(self):
        self.parse_args(sys.argv[1:])
        spark = self.get_spark_session()

        dfs = [self.create_df_from_file(spark, infile) for infile in self.args.infiles]
        df = self.join_df_on(dfs, self.key_column)
        df.write.parquet(self.args.outfiles)

        spark.stop()
