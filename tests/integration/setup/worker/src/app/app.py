# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

from os import (
    environ,
)

from celery import (
    Celery,
)
from celery.utils.log import (
    get_task_logger,
)

from celery_dyrygent.tasks import (
    register_workflow_processor,
)

logger = get_task_logger(__name__)

broker_url = environ['BROKER_URL']
result_backend = environ['RESULT_BACKEND']

app = Celery('tasks', broker=broker_url, backend=result_backend)
workflow_processor = register_workflow_processor(app)


@app.task()
def order_task(test_name, test_value):
    logger.info('Test name: %s Test value: %s', test_name, test_value)
    return


@app.task(bind=True)
def return_value_task(self, test_name):
    logger.info('Test name: %s Test value: %s', test_name, self.request.id)
    return


@app.task()
def read_logs():
    with open('celery.logs', 'r') as reader:
        logs = reader.read()
        return '\n'.join([log for log in logs.splitlines() if 'Test name' in log])
