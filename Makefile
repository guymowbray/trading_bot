install: venv
	pip install -r requirements.txt

test:
	pytest .
