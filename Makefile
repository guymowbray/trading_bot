.PHONY: install lint format test test-market-data fix

venv:
	python -m venv .venv

install: venv
	pip install -e .[dev]

lint:
	ruff check . --fix

lint-isort:
	ruff check . --select I --fix

format:
	ruff format .

test:
	pytest .

test-market-data:
	pytest tests/market_data