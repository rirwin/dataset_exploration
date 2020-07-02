import unittest
from parameterized import parameterized

from batch.base_dataset_creator import BaseDatasetCreatorSparkBatch


class BaseDatasetTestCase(unittest.TestCase):

    def setUp(self):
        self.batch = BaseDatasetCreatorSparkBatch()

    def test_parse_args(self):
        input_sys_args = ['--infiles', 'business.json', 'review.json', 'tip.json', 'checkin.json']
        output_sys_args = ['--outfiles', './out/']
        self.batch.parse_args(input_sys_args + output_sys_args)
        self.assertEqual(self.batch.args.infiles, input_sys_args[1:])
        self.assertEqual(self.batch.args.outfiles, output_sys_args[1])

    @parameterized.expand([
        ['business', 's3://bucket/a_business_file1.json', 'BUSINESS'],
        ['checkin', 's3://bucket/a_checkin_file1.json', 'CHECKIN'],
        ['tip', 's3://bucket/a_tip_file.json1', 'TIP'],
        ['user', 's3://bucket/a_user_file1.json', 'USER'],
        ['review', 's3://bucket/a_review_file1.json', 'REVIEW'],
    ])
    def test_match_infile_to_type(self, name, infile, expected_type):
        self.assertEqual(self.batch.match_infile_to_type(infile), expected_type)

    def test_match_infile_to_type_invalid(self):
        with self.assertRaises(Exception):
            self.batch.match_infile_to_type('s3://blah.json')


if __name__ == '__main__':
    unittest.main()
