# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

import re
import uuid
from collections import (
    defaultdict,
)

import celery
import pytest
from app import (
    order_task,
    read_logs,
    return_value_task,
)

from celery_dyrygent.workflows import (
    Workflow,
)


@pytest.fixture(scope='function')
def test_name():
    return str(uuid.uuid4())


class Helpers:
    @staticmethod
    def parse_logs(logs):
        task_regex = r'app.(?:order|return_value)_task\[([\w-]+)\]: Test name: ([\w-]+) Test value: ([\w-]+)'
        grouped_logs = defaultdict(list)

        for log in logs.splitlines():
            findings = re.search(task_regex, log)
            if findings:
                _, test_name, test_value = findings.groups()
                grouped_logs[test_name].append(test_value)

        return grouped_logs


class TestCeleryDyrygentReturnValue(Helpers):
    def test_return_value(self, test_name):
        canvas = return_value_task.s(test_name)

        workflow = Workflow()
        workflow.add_celery_canvas(canvas)
        workflow.apply_async().wait()

        task_id = list(workflow.nodes.keys())[0]
        logs_result = read_logs.delay()
        results = self.parse_logs(logs_result.get())[test_name]

        assert task_id == results[0]


class TestCeleryDyrygentOrder(Helpers):

    def get_canvas_order(self, canvas):
        workflow = Workflow()
        workflow.add_celery_canvas(canvas)
        result = workflow.apply_async()
        result.wait()

        logs_result = read_logs.delay()
        return self.parse_logs(logs_result.get())

    def test_single_task(self, test_name):
        canvas = order_task.s(test_name, 'task1')
        assert self.get_canvas_order(canvas, )[test_name] == ['task1']

    def test_group(self, test_name):
        canvas = celery.group([order_task.s(test_name, f'task{task_nr}') for task_nr in range(1, 6)])
        assert set(self.get_canvas_order(canvas, )[test_name]) == {'task1', 'task2', 'task3', 'task4', 'task5'}

    def test_chord(self, test_name):
        canvas = celery.chord([order_task.s(test_name, f'task{task_nr}', ) for task_nr in range(1, 6)],
                              order_task.s(test_name, 'last'))
        assert (self.get_canvas_order(canvas, )[test_name])[-1] == 'last'

    def test_chain(self, test_name):
        canvas = celery.chain(
            order_task.s(test_name, 'task1'),
            order_task.s(test_name, 'task2'),
            order_task.s(test_name, 'task3')
        )
        assert (self.get_canvas_order(canvas, )[test_name]) == ['task1', 'task2', 'task3']

    def test_combination(self, test_name):
        canvas = celery.group(order_task.s(test_name, 'task1a'), order_task.s(test_name, 'task1b')) | \
                 order_task.s(test_name, 'task2') | order_task.s(test_name, 'task3') | \
                 celery.group(order_task.s(test_name, 'task4a'), order_task.s(test_name, 'task4b')) | \
                 order_task.s(test_name, 'task5')
        order = self.get_canvas_order(canvas, )[test_name]
        assert set(order[:2]) == {'task1a', 'task1b'}
        assert order[2:4] == ['task2', 'task3']
        assert set(order[4:6]) == {'task4a', 'task4b'}
        assert order[6] == 'task5'
