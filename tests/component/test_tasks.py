# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

import mock

from celery_dyrygent import (
    tasks,
)
from celery_dyrygent.celery import (
    entities,
)
from celery_dyrygent.workflows import (
    Workflow,
)


class TestTasks(object):
    def test_register_workflow_processor(self):
        app = entities.Celery()
        with (
            mock.patch('celery_dyrygent.tasks.processor.workflow_processor')
        ) as mck:
            mck.__name__ = 'test-name'
            mck.__annotations__ = []
            task = tasks.register_workflow_processor(app)
            task('test')

        mck.assert_called_with('test')
        assert task.name == 'workflow-processor'

        assert Workflow.workflow_processor_task is task

    def test_register_workflow_processor_decorator_kws(self):
        app = mock.Mock()
        tasks.register_workflow_processor(
            app, bind=False, name='test', max_retries=5)

        app.task.assert_called_with(bind=False, name='test', max_retries=5)
