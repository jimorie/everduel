VENV := venv
SRC := everduel persisthing test

.PHONY: venv
venv:
	python3 -m venv ${VENV}
	${VENV}/bin/pip install --upgrade pip
	${VENV}/bin/pip install -r requirements.txt -r requirements-dev.txt

.PHONY: lint
lint:
	${VENV}/bin/flake8 ${SRC}
	${VENV}/bin/black --check ${SRC}

.PHONY: format
format:
	${VENV}/bin/black ${SRC}

.PHONY: test
test:
	PYTHONPATH=. ${VENV}/bin/pytest test
