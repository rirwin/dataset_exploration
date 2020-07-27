import mock
import unittest
import shutil

from batch.business_dataset_creator import BusinessDatasetCreatorSparkBatch


class BusinessDatasetTestCase(unittest.TestCase):

    def setUp(self):
        self.batch = BusinessDatasetCreatorSparkBatch()

    def test_get_spark_session(self):
        # TODO rewrite to decorator
        try:
            shutil.rmtree('test_files/test_output')
        except:
            pass
        mock_sys = mock.Mock()
        mock_sys.argv = [0,
            '--infiles', 'test_files/business_test.json', 'test_files/review_test.json',
            '--outfiles', 'test_files/test_output/'
        ]
        with mock.patch('batch.base_dataset_creator.sys', mock_sys):
            self.batch.run()
        try:
            shutil.rmtree('test_files/test_output')
        except:
            pass


if __name__ == '__main__':
    unittest.main()
