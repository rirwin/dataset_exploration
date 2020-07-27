from typing import Dict, List


from batch.base_dataset_creator import BaseDatasetCreatorSparkBatch  # type: ignore


class UserDatasetCreatorSparkBatch(BaseDatasetCreatorSparkBatch):

    key_column = 'user_id'

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


if __name__ == "__main__":
    UserDatasetCreatorSparkBatch().run()
