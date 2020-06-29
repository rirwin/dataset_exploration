virtualenv:
	virtualenv -p python3.8 virtualenv_run
	virtualenv_run/bin/pip install -r requirements.txt

test: virtualenv
	python -m tests.test_dataset_creator 
	mypy batch/dataset_creator.py
	coverage run --source=batch/ -m unittest
	coverage report -m

run_dev_test: virtualenv 
	rm -rf test_write/
	virtualenv_run/bin/python -m batch.dataset_creator \
		--infiles test_files/business_test.json test_files/review_test.json \
		test_files/tip_test.json test_files/checkin_test.json
	virtualenv_run/bin/python -m batch.parquet_reader

run_small_test: virtualenv
	rm -rf test_write/
	virtualenv_run/bin/python -m batch.dataset_creator \
		--infiles test_files/business_small.json test_files/review_small.json \
		test_files/tip_small.json test_files/checkin_small.json
	virtualenv_run/bin/python -m batch.parquet_reader

# TODO python black

clean:
	rm -f .coverage
	rm -rf virtualenv_run/
