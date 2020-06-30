virtualenv:
	virtualenv -p python3.8 virtualenv_run
	virtualenv_run/bin/pip install -r requirements.txt

test: virtualenv
	python -m tests.test_business_dataset_creator 
	mypy batch/business_dataset_creator.py
	coverage run --source=batch/ -m unittest
	coverage report -m

run_dev_test: virtualenv 
	rm -rf test_business_out/
	virtualenv_run/bin/python -m batch.business_dataset_creator \
		--infiles test_files/business_test.json test_files/review_test.json \
		    test_files/tip_test.json test_files/checkin_test.json \
        --outfiles test_business_out
	virtualenv_run/bin/python -m batch.parquet_reader test_business_out

run_small_test: virtualenv
	rm -rf test_business_write/
	virtualenv_run/bin/python -m batch.business_dataset_creator \
		--infiles test_files/business_small.json test_files/review_small.json \
		          test_files/tip_small.json test_files/checkin_small.json \
	virtualenv_run/bin/python -m batch.parquet_reader test_business_out

# TODO python black

clean:
	rm -f .coverage
	rm -rf virtualenv_run/
