.PHONY: help setup run test clean

help:
	@echo "Targets:"
	@echo "  make setup  - create venv and install project"
	@echo "  make run    - one-click demo run (download + write all formats)"
	@echo "  make test   - run tests"
	@echo "  make clean  - remove local build/cache artifacts"

setup:
	python3 -m venv .venv
	.venv/bin/pip install -U pip
	.venv/bin/pip install -e .

run:
	./scripts/run_demo.sh

test:
	.venv/bin/pip install -e ".[dev]"
	PYTHONPATH=src .venv/bin/pytest -q

clean:
	rm -rf __pycache__ .pytest_cache data_lake
