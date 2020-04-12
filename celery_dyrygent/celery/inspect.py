# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

from celery_dyrygent.celery import (
    entities,
)


def get_chain_tasks(chain_instance):
    """Get chain tasks
        Return list of tasks in chain
    """
    return chain_instance.tasks


def get_chord_tasks(chord_instance):
    """Get chord tasks
        Return (body task, list of tasks in header)
    """
    return chord_instance.body, chord_instance.tasks


def get_group_tasks(group_instance):
    """Get tasks in group
        Return list of tasks in group
    """
    return group_instance.tasks


def get_task_state(task_id):
    """Get celery async result of given task
    """
    return ResultState(entities.AsyncResult(task_id))


class ResultState(object):
    INIT = 0
    RUNNING = 1
    DONE = 2
    RESCHEDULE = 3
    FAILED_RETRYING = 4

    def __init__(self, celery_result):
        self.result = celery_result
        self.state = None
        self.success = None
        self.done = None

        self.__inspect()

    def __inspect(self):
        self.success = self.result.successful()
        if self.result.state == 'PENDING':
            self.state = self.INIT

        elif self.result.ready():
            self.state = self.DONE
            self.done = True
        else:
            self.state = self.RUNNING
            self.done = False

        # due to celery bugs state may be overwritten

        # mitigation for celery bug
        # https://github.com/celery/celery/issues/5120
        # see also is_final method
        if (
            isinstance(self.result.result, entities.WorkerLostError)
            and (
                "Worker exited prematurely: exitcode 155"
                in str(self.result.result)
            )
        ):
            self.state = self.FAILED_RETRYING

        # mitigation for some rare celery bug
        # TypeError: unhashable type: 'list'
        # File /python2.7/site-packages/celery/canvas.py", line 476, in type
        # return self._type or self.app.tasks[self['task']]
        elif (
            isinstance(self.result.result, TypeError)
            and "unhashable type: 'list'" == str(self.result.result)
        ):
            self.state = self.RESCHEDULE

    def is_successful(self):
        return self.success

    def is_done(self):
        return self.done

    def was_published(self):
        """
        Checks if task was ever published for execution or not.
        This is not 100% accurate as scheduled tasks waiting in queue
        have also PENDING state.
        """
        return self.state != self.INIT

    def needs_reschedule(self):
        return self.state == self.RESCHEDULE

    def is_final(self):
        """Check if task result is final
        Due to bug in celery https://github.com/celery/celery/issues/5120
        Tasks might be incorrectly marked as failed even if succeeded or
        retried If task result is instance of WorkerLost and message contains
        exitcode 155 consider task state as not final and hope for the state
        to be changed.
        """
        return self.state in (self.DONE, self.RESCHEDULE)

    @property
    def value(self):
        return self.result.result

    @property
    def task_state(self):
        return self.result.state
