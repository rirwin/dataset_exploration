virtualenv:
	virtualenv -p python3.8 virtualenv_run
	virtualenv_run/bin/pip install -r requirements.txt

test: virtualenv
	python -m unittest discover tests/
	mypy batch/*.py
	coverage run --source=batch/ -m unittest
	coverage report -m

run_dev_test_business: virtualenv 
	rm -rf test_business_unstructured_out/
	rm -rf test_business_structured_out/
	virtualenv_run/bin/python -m batch.business_dataset_creator \
		--infiles test_files/business_test.json test_files/review_test.json \
		    test_files/tip_test.json test_files/checkin_test.json \
        --outfiles test_business_unstructured_out
	virtualenv_run/bin/python -m batch.business_structured_features \
		--infiles test_business_unstructured_out \
		--outfiles test_business_structured_out
	virtualenv_run/bin/python -c "from dataset_client.business_client import BusinessClient; print(BusinessClient().get_df().head())"

run_dev_test_user: virtualenv
	rm -rf test_user_unstructured_out/
	rm -rf test_user_structured_out/
	virtualenv_run/bin/python -m batch.user_dataset_creator \
		--infiles test_files/user_test.json test_files/review_test.json test_files/tip_test.json \
		--outfiles test_user_unstructured_out
	virtualenv_run/bin/python -m batch.user_structured_features \
		--infiles test_user_unstructured_out \
		--outfiles test_user_structured_out
	virtualenv_run/bin/python -c "from dataset_client.user_client import UserClient; print(UserClient().get_df().head())"

run_dev_small_business: virtualenv 
	rm -rf test_business_unstructured_out/
	rm -rf test_business_structured_out/
	virtualenv_run/bin/python -m batch.business_dataset_creator \
		--infiles test_files/business_small.json test_files/review_small.json \
		    test_files/tip_small.json test_files/checkin_small.json \
        --outfiles test_business_unstructured_out
	virtualenv_run/bin/python -m batch.business_structured_features \
		--infiles test_business_unstructured_out \
		--outfiles test_business_structured_out
	virtualenv_run/bin/python -c "from dataset_client.business_client import BusinessClient; print(BusinessClient().get_df().head())"

start_es: virtualenv
	virtualenv_run/bin/docker-compose -f docker/elasticsearch/docker-compose.yml up -d

stop_es: virtualenv
	virtualenv_run/bin/docker-compose -f docker/elasticsearch/docker-compose.yml down 

load_es_review_search: virtualenv
	virtualenv_run/bin/python -m batch.load_es_review_search

clean:
	rm -f .coverage
	rm -rf virtualenv_run/
