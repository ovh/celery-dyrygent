# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

import mock
import pytest

from celery_dyrygent import (
    tasks,
)
from celery_dyrygent.workflows import Workflow


class TestTasks(object):
    @pytest.fixture
    def task_obj(self):
        return mock.Mock(name='task',
                         request=mock.Mock(id='dead-beef', retries=1))

    @pytest.fixture
    def wf(self):
        return mock.Mock()

    @pytest.fixture
    def from_dict(self, wf):
        with mock.patch.object(Workflow, 'from_dict', return_value=wf) as mck:
            yield mck

    def test_workflow_processor(self, task_obj, from_dict, wf):
        wf.tick.return_value = False
        tasks.workflow_processor(task_obj, {'a': 'b'})
        from_dict.assert_called_with({'a': 'b', 'id': task_obj.request.id})
        wf.tick.assert_called_with()
        task_obj.retry.assert_not_called()

        wf.tick.return_value = True

        # task run will decrease retries to retry till processing
        # timestamp is reached or all processing done
        tasks.workflow_processor(task_obj, {'a': 'b'})
        task_obj.retry.assert_called_with(
            kwargs=dict(workflow_dict=wf.to_dict()),
            countdown=wf.get_retry_countdown(),
        )
        assert task_obj.request.retries == 0
