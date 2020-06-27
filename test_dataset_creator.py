import unittest
from parameterized import parameterized

from dataset_creator import match_infile_to_type
from dataset_creator import parse_args
from dataset_creator import process_dict


BIZ_ID = 'IXAV123'
USER_ID = 'UXAV456'
REVIEW_ID = 'RXAV789'
NAME = 'Imos Pizza'
CITY = 'St. Louis'
STATE = 'Missouri'
TS = '2015-08-09 03:14:00'
TS2 = '2020-03-09 13:14:10'
TEXT = 'Great Pizza'


class DatasetTestCase(unittest.TestCase):
    
    def test_parse_args(self):
        infiles = 'business.json review.json tip.json checkin.json'
        args = parse_args(['--infiles'] + infiles.split(' '))
        self.assertEqual(args.infiles, infiles.split(' '))

    @parameterized.expand([
        ['business', 's3://bucket/a_business_file1.json', 'BUSINESS'],
        ['checkin', 's3://bucket/a_checkin_file1.json', 'CHECKIN'],
        ['tip', 's3://bucket/a_tip_file.json1', 'TIP'],
        ['review', 's3://bucket/a_review_file1.json', 'REVIEW'],
    ])
    def test_match_infile_to_type(self, name, infile, expected_type):
        self.assertEqual(match_infile_to_type(infile), expected_type)
 
    @parameterized.expand([
        [
            {'business_id': BIZ_ID, 'name': NAME, 'city': CITY, 'state': STATE},
            'BUSINESS',
            [{'business_id': BIZ_ID, 'name': NAME, 'city': CITY, 'state': STATE}]
        ],
        [    
            {'business_id': BIZ_ID, 'user_id': USER_ID, 'review_id': REVIEW_ID, 'date': TS, 'stars': 5, 'text':TEXT},
            'REVIEW',
            [{'business_id': BIZ_ID, 'user_id': USER_ID, 'review_id': REVIEW_ID, 'timestamp': TS, 'star_rating': 5, 'text':TEXT}]
        ],
        [
            {'business_id': BIZ_ID, 'date': "{0}, {1}".format(TS, TS2)},
            'CHECKIN',
            [[TS, TS2]]
        ],
        [
            {'business_id': BIZ_ID, 'user_id': USER_ID, 'text': TEXT, 'date': TS},
            'TIP',
            [{'business_id': BIZ_ID, 'user_id': USER_ID, 'text': TEXT, 'timestamp': TS}],
        ]
    ])
    def test_process_dict(self, d, d_type, expected): 
        self.assertEqual(process_dict(d, d_type), expected)


if __name__ == '__main__':
        unittest.main()
