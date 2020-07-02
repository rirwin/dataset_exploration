import unittest
from parameterized import parameterized

from batch.user_dataset_creator import UserDatasetCreatorSparkBatch


BIZ_ID = 'IXAV123'
USER_ID = 'UXAV456'
REVIEW_ID = 'RXAV789'
NAME = 'Tony'
CITY = 'St. Louis'
STATE = 'Missouri'
TS = '2015-08-09 03:14:00'
TS2 = '2020-03-09 13:14:10'
TEXT = 'Great Pizza'
CATS = 'Restaurants, Pizza'
REV_CNT = 10
AVG_RTG = 4.5


class UserDatasetTestCase(unittest.TestCase):

    def setUp(self):
        self.batch = UserDatasetCreatorSparkBatch()

    @parameterized.expand([
        [
            {'user_id': USER_ID, 'name': NAME, 'review_count': REV_CNT, 'average_stars': AVG_RTG},
            'USER',
            [{'user_id': USER_ID, 'name': NAME, 'review_count': REV_CNT, 'average_rating': AVG_RTG}]
        ],
        [
            {'business_id': BIZ_ID, 'user_id': USER_ID, 'review_id': REVIEW_ID, 'date': TS, 'stars': 5, 'text':TEXT},
            'REVIEW',
            [{'business_id': BIZ_ID, 'user_id': USER_ID, 'review_id': REVIEW_ID, 'timestamp': TS, 'star_rating': 5, 'text':TEXT}]
        ],
        [
            {'business_id': BIZ_ID, 'user_id': USER_ID, 'text': TEXT, 'date': TS},
            'TIP',
            [{'business_id': BIZ_ID, 'user_id': USER_ID, 'text': TEXT, 'timestamp': TS}],
        ]
    ])
    def test_process_dict(self, d, d_type, expected):
        self.assertEqual(self.batch.process_dict(d, d_type), expected)


if __name__ == '__main__':
    unittest.main()
