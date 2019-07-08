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

regen_license:
	egrep -Lr --include=*.py '# Use of this source code is governed by a BSD-style' celery_dyrygent | xargs -n 1 -I {} sed -i "1s;^;# Use of this source code is governed by a BSD-style\n# license that can be found in the LICENSE file.\n# Copyright 2019 The celery-dyrygent Authors. All rights reserved.\n\n;" {}
	egrep -Lr --include=*.py '# Use of this source code is governed by a BSD-style' tests | xargs -n 1 -I {} sed -i "1s;^;# Use of this source code is governed by a BSD-style\n# license that can be found in the LICENSE file.\n# Copyright 2019 The celery-dyrygent Authors. All rights reserved.\n\n;" {}
