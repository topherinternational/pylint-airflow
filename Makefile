.PHONY: black
black:
	find . -name '*.py' ! -path './venv/*' ! -path './build/*' | xargs black --check --line-length=100

.PHONY: pylint
pylint:
	find . -name '*.py' ! -path './venv/*' ! -path './build/*' | xargs pylint --output-format=colorized

.PHONY: validate_message_ids
validate_message_ids:
	python scripts/ci_validate_msg_ids.py

.PHONY: pytest
pytest:
	pytest tests/ -W ignore::DeprecationWarning

.PHONY: clean-compiled
clean-compiled:  # Remove Python artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -rf {} +

.PHONY: ci
ci: | clean-compiled black pylint validate_message_ids pytest

.PHONY: ci-docker
ci-docker: build_ci_image
	docker run -ti -v `pwd`:/pylint-airflow -w /pylint-airflow pylint-airflow/pylint-airflow-ci:3.7-slim bash -c "pip install .; make ci"
	docker run -ti -v `pwd`:/pylint-airflow -w /pylint-airflow pylint-airflow/pylint-airflow-ci:3.8-slim bash -c "pip install .; make ci"

.PHONY: build_ci_image
build_ci_image:
	docker build --file docker/ci.Dockerfile --build-arg PYTHON_VERSION=3.7-slim --tag pylint-airflow/pylint-airflow-ci:3.7-slim .
	docker build --file docker/ci.Dockerfile --build-arg PYTHON_VERSION=3.8-slim --tag pylint-airflow/pylint-airflow-ci:3.8-slim .

.PHONY: upload-to-pypi
upload-to-pypi:
	rm -rf dist/
	python setup.py sdist bdist_wheel
	twine check dist/*
	twine upload dist/*
