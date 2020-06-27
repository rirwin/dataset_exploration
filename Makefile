virtualenv:
	virtualenv virtualenv_run
	virtualenv_run/bin/pip install -r requirements.txt

run_dev_test: virtualenv 
	virtualenv_run/bin/python -m dataset_creator
	rm -rf test_write/

clean:
	rm -rf virtualenv_run/
