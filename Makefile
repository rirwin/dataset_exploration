virtualenv:
	virtualenv virtualenv_run
	virtualenv_run/bin/pip install -r requirements.txt

test: virtualenv
	python -m test_dataset_creator 

run_dev_test: virtualenv 
	rm -rf test_write/
	virtualenv_run/bin/python -m dataset_creator --infiles business_test.json review_test.json tip_test.json checkin_test.json

# TODO python black and types

clean:
	rm -rf virtualenv_run/
