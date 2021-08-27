PYTHON_VERSION := '3.6'
CELERY_VERSION := '4.2.0'

ifdef COV
	override PYTEST_OPTS += --cov=celery_dyrygent
endif

clean:
	rm -rf dist
	rm -rf build

bdist:
	python setup.py bdist_wheel

test: test_unit test_component

test_unit:
	py.test $(PYTEST_OPTS) tests/unit

test_component:
	py.test $(PYTEST_OPTS) tests/component

test_integration:
	rm -r  tests/integration/setup/worker/src/celery_dyrygent || true
	cp -r celery_dyrygent tests/integration/setup/worker/src/
	docker-compose -p integration_test -f tests/integration/setup/docker-compose.yml build --no-cache --build-arg CELERY_VERSION=$(CELERY_VERSION) --build-arg PYTHON_VERSION=$(PYTHON_VERSION)
	docker-compose -p integration_test -f tests/integration/setup/docker-compose.yml up --detach
	BROKER_URL='amqp://admin:mypass@localhost:5672//' RESULT_BACKEND='redis://localhost:6379/0' PYTHONPATH="$$(pwd)/:$$(pwd)/tests/integration/setup/worker/src/app" py.test tests/integration
	docker-compose -f tests/integration/setup/docker-compose.yml stop

regen_license:
	egrep -Lr --include=*.py '# Use of this source code is governed by a BSD-style' celery_dyrygent | xargs -n 1 -I {} sed -i "1s;^;# Use of this source code is governed by a BSD-style\n# license that can be found in the LICENSE file.\n# Copyright 2019 The celery-dyrygent Authors. All rights reserved.\n\n;" {}
	egrep -Lr --include=*.py '# Use of this source code is governed by a BSD-style' tests | xargs -n 1 -I {} sed -i "1s;^;# Use of this source code is governed by a BSD-style\n# license that can be found in the LICENSE file.\n# Copyright 2019 The celery-dyrygent Authors. All rights reserved.\n\n;" {}
