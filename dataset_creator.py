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
        .map(lambda d: (d['business_id'], process_dict(d, infile_to_type[infile]))) \
        .reduceByKey(lambda data1, data2: data1 + data2)
    return spark.createDataFrame(rdd, ['business_id', infile_to_type[infile]])

def join_df_on_business_id(dfs):
    return reduce(lambda df1, df2: df1.join(df2, ['business_id'], 'left_outer'), dfs)


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("YelpBusinessMapper")\
        .getOrCreate()

    infile_to_type = {
        'business_small.json': 'BUSINESS',
        'business_test.json': 'BUSINESS',
        'review_small.json': 'REVIEW',
        'review_test.json': 'REVIEW',
        'checkin_small.json': 'CHECKIN',
        'checkin_test.json': 'CHECKIN',
        'tip_test.json': 'TIP',
    }

    business_df = create_df_from_file(spark, 'business_test.json')
    business_reviews_df = create_df_from_file(spark, 'review_test.json')
    business_checkins_df = create_df_from_file(spark, 'checkin_test.json')
    business_tips_df = create_df_from_file(spark, 'tip_test.json')

    df = join_df_on_business_id([
        business_df, 
        business_reviews_df, 
        business_checkins_df,
        business_tips_df,
    ])

    # consider repartion
    df.write.json('test_write')

    spark.stop()
