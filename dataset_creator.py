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


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    #sc = spark.sparkContext
    #df = spark.read.json(sys.argv[1])
    #df.printSchema()

    infile_to_type = {
        'business_small.json': 'BUSINESS',
        'business_test.json': 'BUSINESS',
        'review_small.json': 'REVIEW',
        'review_test.json': 'REVIEW',
        'checkin_small.json': 'CHECKIN',
    }
    #lines = spark.read.json(sys.argv[1], multiLine=True)
    def process_dict(d, d_type):
        business_id = d['business_id']
        if d_type == 'BUSINESS':
            entry = {
                'business_id': business_id, 
                'name': d['name'], 
                'city': d['city'], 
                'state': d['state']
            }
        elif d_type == 'REVIEW':
            entry = {
                'business_id': business_id,
                'review_id': d['review_id'],
                'timestamp': d['date'],
                'star_rating': d['stars'],
                'text': d['text']
            }

        return (business_id, [entry])

    def create_df_from_file(spark, infile):
        rdd = spark.read.json(infile).rdd \
            .map(lambda d: (d['business_id'], process_dict(d, infile_to_type[infile]))) \
            .reduceByKey(lambda data1, data2: data1 + data2)
        return spark.createDataFrame(rdd, ['business_id', infile_to_type[infile]])

    def join_df_on_business_id(dfs):
        return reduce(lambda df1, df2: df1.join(df2, ['business_id'], 'left_outer'), dfs)

    business_df = create_df_from_file(spark, 'business_test.json')
    business_reviews_df = create_df_from_file(spark, 'review_test.json')

    df = join_df_on_business_id([business_df, business_reviews_df])

    df.write.json('test_write')

    #lines = spark.read.json(list(infile_to_type.keys())).rdd.map(lambda r: (r['business_id'], r))
    #lines.saveAsTextFile('./test_file')
    #lines.collect().foreach(println)
    #counts = lines.flatMap(lambda x: x.split(' ')) \
    #              .map(lambda x: (x, 1)) \
    #              .reduceByKey(add)
    #output = counts.collect()
    #for (word, count) in output:
        #print("%s: %i" % (word, count))

    spark.stop()
