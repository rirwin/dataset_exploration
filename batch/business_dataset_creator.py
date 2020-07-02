from typing import Dict, List

from batch.base_dataset_creator import BaseDatasetCreatorSparkBatch  # type: ignore


class BusinessDatasetCreatorSparkBatch(BaseDatasetCreatorSparkBatch):

    key_column = 'business_id'

    def process_dict(self, d: Dict[str, str], d_type: str) -> List:
        if d_type == 'BUSINESS':
            entry = {
                'business_id': d['business_id'],
                'name': d['name'],
                'city': d['city'],
                'state': d['state'],
                'categories': d['categories'],
                'star_rating': d['stars'],
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
            entry = {
                'business_id': d['business_id'],
                'timestamps': d['date']
            }
        elif d_type == 'TIP':
            entry = {
                'business_id': d['business_id'],
                'user_id': d['user_id'],
                'text': d['text'],
                'timestamp': d['date'],
            }
        return [entry]


if __name__ == "__main__":
    BusinessDatasetCreatorSparkBatch().run()
