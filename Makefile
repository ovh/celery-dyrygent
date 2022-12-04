ifdef COV
	override PYTEST_OPTS += --cov=celery_dyrygent
endif

clean:
	rm -rf dist
	rm -rf build

bdist:
	python setup.py bdist_wheel

requirements_dev:
	pip install -r requirements-dev.txt 

test: test_unit test_component

test_unit:
	py.test $(PYTEST_OPTS) tests/unit

test_component:
	py.test $(PYTEST_OPTS) tests/component

# ------------------------------------- INTEGRATION TESTS -------------------------------------
# ---------------------------------------------------------------------------------------------

PYTHON_VERSION ?= '3.8'
CELERY_VERSION ?= '<6'
CELERY_BACKEND ?= redis

export PYTHON_VERSION
export CELERY_VERSION
export CELERY_BACKEND

ifeq ($(CELERY_BACKEND), redis)
RESULT_BACKEND := 'redis://redis/0'
RESULT_BACKEND_LOCAL := 'redis://localhost/0'
endif

ifeq ($(CELERY_BACKEND), sqlalchemy)
RESULT_BACKEND := 'db+mysql+pymysql://user:pass@mysql/celery'
RESULT_BACKEND_LOCAL := 'db+mysql+pymysql://user:pass@localhost/celery'
endif

requirements_test_integration: requirements_dev
	pip install celery[$(CELERY_BACKEND)]$(CELERY_VERSION)
	pip install PyMySQL[rsa]

test_integration: requirements_test_integration
	rm -r  tests/integration/setup/worker/src/celery_dyrygent || true
	cp -r celery_dyrygent tests/integration/setup/worker/src/
	docker-compose -p integration_test -f tests/integration/setup/docker-compose.yml build --no-cache --build-arg CELERY_VERSION=$(CELERY_VERSION) --build-arg PYTHON_VERSION=$(PYTHON_VERSION) --build-arg CELERY_BACKEND=$(CELERY_BACKEND)
	RESULT_BACKEND=$(RESULT_BACKEND) docker-compose -p integration_test -f tests/integration/setup/docker-compose.yml up --force-recreate --detach
	RESULT_BACKEND=$(RESULT_BACKEND_LOCAL) BROKER_URL='amqp://admin:mypass@localhost//' PYTHONPATH="$$(pwd)/:$$(pwd)/tests/integration/setup/worker/src/app" timeout 300 py.test -vs tests/integration
	docker-compose -f tests/integration/setup/docker-compose.yml stop

# ---------------------------------------------------------------------------------------------

regen_license:
	egrep -Lr --include=*.py '# Use of this source code is governed by a BSD-style' celery_dyrygent | xargs -n 1 -I {} sed -i "1s;^;# Use of this source code is governed by a BSD-style\n# license that can be found in the LICENSE file.\n# Copyright 2019 The celery-dyrygent Authors. All rights reserved.\n\n;" {}
	egrep -Lr --include=*.py '# Use of this source code is governed by a BSD-style' tests | xargs -n 1 -I {} sed -i "1s;^;# Use of this source code is governed by a BSD-style\n# license that can be found in the LICENSE file.\n# Copyright 2019 The celery-dyrygent Authors. All rights reserved.\n\n;" {}

.PHONY: bdist clean regen_license requirements_dev requirements_test_integration test test_component test_integration test_unit
