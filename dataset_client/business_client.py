from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import List
import json


PATHS = ['test_business_unstructured_out', 'test_business_structured_out']  # First item should be all user ids, or consider outer join
PATH_TO_SCHEMA = {
    'test_business_unstructured_out': '{"fields":[{"metadata":{},"name":"business_id","nullable":true,"type":"string"},{"metadata":{},"name":"BUSINESS","nullable":true,"type":{"containsNull":true,"elementType":{"keyType":"string","type":"map","valueContainsNull":true,"valueType":"string"},"type":"array"}},{"metadata":{},"name":"REVIEW","nullable":true,"type":{"containsNull":true,"elementType":{"keyType":"string","type":"map","valueContainsNull":true,"valueType":"string"},"type":"array"}},{"metadata":{},"name":"TIP","nullable":true,"type":{"containsNull":true,"elementType":{"keyType":"string","type":"map","valueContainsNull":true,"valueType":"string"},"type":"array"}},{"metadata":{},"name":"CHECKIN","nullable":true,"type":{"containsNull":true,"elementType":{"keyType":"string","type":"map","valueContainsNull":true,"valueType":"string"},"type":"array"}}],"type":"struct"}',
    'test_business_structured_out': '{"fields":[{"metadata":{},"name":"business_id","nullable":true,"type":"string"},{"metadata":{},"name":"num_checkins","nullable":true,"type":"long"},{"metadata":{},"name":"num_tips","nullable":true,"type":"long"},{"metadata":{},"name":"num_reviews","nullable":true,"type":"long"},{"metadata":{},"name":"good_review_count","nullable":true,"type":"long"},{"metadata":{},"name":"business_name","nullable":true,"type":"string"},{"metadata":{},"name":"state","nullable":true,"type":"string"},{"metadata":{},"name":"city","nullable":true,"type":"string"},{"metadata":{},"name":"is_rfn","nullable":true,"type":"boolean"}],"type":"struct"}'
}


class BusinessClient(object):

    cols = None
    df = None

    def __init__(self):
        self.spark = SparkSession.builder.appName("DatasetClient").getOrCreate()  # TODO make config, and per (linux) user
        for path in PATHS:
            if self.df is None:
                self.df = self.spark.read.schema(StructType.fromJson(json.loads(PATH_TO_SCHEMA[path]))).parquet(path)
            else:
                new_df = self.spark.read.schema(StructType.fromJson(json.loads(PATH_TO_SCHEMA[path]))).parquet(path)
                self.df = self.df.join(new_df, on='business_id')

    def get_df(self):
        return self.df
