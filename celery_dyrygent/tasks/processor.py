# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

from celery_dyrygent.workflows import Workflow


def workflow_processor(self, workflow_dict):
    workflow_dict['id'] = self.request.id
    wf = Workflow.from_dict(workflow_dict)
    still_running = wf.tick()

    if still_running:
        # we can retry infinitely due to processing limit timestamp
        self.request.retries -= 1
        self.retry(
            kwargs=dict(workflow_dict=wf.to_dict()),
            countdown=wf.get_retry_countdown(),
        )


def register_workflow_processor(app, **decorator_kws):
    """
    Attach workflow tasks to given celery app instance
    """
    decorator_defaults = dict(
        name='workflow-processor',
        bind=True,
        max_retries=1,
    )
    if decorator_kws is not None:
        decorator_defaults.update(decorator_kws)

    task = app.task(
        **decorator_defaults
    )(workflow_processor)

    Workflow.set_workflow_processor_task(task)
    return task
