from datetime import datetime
from elasticsearch import Elasticsearch
from dataset_client.business_client import BusinessClient


es = Elasticsearch()
df = BusinessClient().get_df()

# doc = {
#  'business_id': 'ABC',
#  'business_name': 'Decent Pizza',
#  'review_text': 'a review'
# }

# not bulk
id_ = 0
for row in df.rdd.collect():
    business_id = row['BUSINESS'][0]['business_id']
    business_name = row['BUSINESS'][0]['name']
    for review in row['REVIEW']:
        doc = {
            'business_name': business_name,
            'business_id': business_id,
            'review_text': review['text']
        }
        es.index(index="review_search", id=id_, body=doc)
        id_ += 1


res = es.search(index="review_search", body={"query": {"match": {"review_text": { "query": "green curry", "fuzziness": "2" }}}})
for hit in res['hits']['hits']:
    print(hit)
