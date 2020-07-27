import mock
import unittest
from parameterized import parameterized

from batch.business_dataset_creator import BusinessDatasetCreatorSparkBatch


BIZ_ID = 'IXAV123'
USER_ID = 'UXAV456'
REVIEW_ID = 'RXAV789'
NAME = 'Imos Pizza'
CITY = 'St. Louis'
STATE = 'Missouri'
TS = '2015-08-09 03:14:00'
TS2 = '2020-03-09 13:14:10'
TEXT = 'Great Pizza'
CATS = 'Restaurants, Pizza'
RTG = 4.5


class BusinessDatasetTestCase(unittest.TestCase):

    def setUp(self):
        self.batch = BusinessDatasetCreatorSparkBatch()

    @parameterized.expand([
        [
            {'business_id': BIZ_ID, 'name': NAME, 'city': CITY, 'state': STATE, 'categories': CATS, 'stars': RTG},
            'BUSINESS',
            [{'business_id': BIZ_ID, 'name': NAME, 'city': CITY, 'state': STATE, 'categories': CATS, 'star_rating': RTG}]
        ],
        [
            {'business_id': BIZ_ID, 'user_id': USER_ID, 'review_id': REVIEW_ID, 'date': TS, 'stars': 5, 'text':TEXT},
            'REVIEW',
            [{'business_id': BIZ_ID, 'user_id': USER_ID, 'review_id': REVIEW_ID, 'timestamp': TS, 'star_rating': 5, 'text':TEXT}]
        ],
        [
            {'business_id': BIZ_ID, 'date': "{0}, {1}".format(TS, TS2)},
            'CHECKIN',
            [{'business_id': BIZ_ID, 'timestamps': "{0}, {1}".format(TS, TS2)}]
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
