#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function
from functools import reduce

import sys
from operator import add

from pyspark.sql import SparkSession


INFILES = [
    'business_test.json',  # business should be first, it's left joined later
    'review_test.json', 
    'checkin_test.json', 
    'tip_test.json'
]


INFILE_TO_TYPE = {
    'business_small.json': 'BUSINESS',
    'business_test.json': 'BUSINESS',
    'review_small.json': 'REVIEW',
    'review_test.json': 'REVIEW',
    'checkin_small.json': 'CHECKIN',
    'checkin_test.json': 'CHECKIN',
    'tip_test.json': 'TIP',
}


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
    rdd = spark.read.json(infile).rdd \
        .map(lambda d: (d['business_id'], process_dict(d, INFILE_TO_TYPE[infile]))) \
        .reduceByKey(lambda data1, data2: data1 + data2)
    return spark.createDataFrame(rdd, ['business_id', INFILE_TO_TYPE[infile]])


def join_df_on_business_id(dfs):
    return reduce(lambda df1, df2: df1.join(df2, ['business_id'], 'left_outer'), dfs)


if __name__ == "__main__":

    spark = SparkSession.builder.appName("YelpBusinessMapper").getOrCreate()

    dfs = [create_df_from_file(spark, infile) for infile in INFILES]
    df = join_df_on_business_id(dfs)
    df.write.json('test_write')

    spark.stop()
