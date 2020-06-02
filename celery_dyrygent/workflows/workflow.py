# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

import logging
import random
import time
from datetime import (
    timedelta,
)

from celery_dyrygent.celery import (
    entities,
    inspect,
)

from celery_dyrygent.workflows.exceptions import WorkflowException
from celery_dyrygent.workflows.node import WorkflowNode


# TO CONSIDER Perhaps following class shouldn't be a mixin but some kind of
# celery canvas to workflow transformer. For now lets leave it as mixin
# since workflows are quite closely bound to celery.
# Mixin only improves readability of Workflow class as the code related
# to understanding celery canvases resides only here
class CeleryWorkflowMixin(object):
    """
    Mixin like class adding celery-workflow understanding
    to Workflow class.
    """
    def add_celery_canvas(self, canvas, dependencies=None):
        """
        Consume any celery canvas.
        Invokes proper methods based on canvas type
        """
        type_map = {
            entities.Signature: self.add_celery_signature,
            entities.Chord: self.add_celery_chord,
            entities.Chain: self.add_celery_chain,
            entities.Group: self.add_celery_group,
        }
        handler = type_map[type(canvas)]
        return handler(canvas, dependencies)

    def add_celery_signature(self, signature, dependencies=None):
        # signatures need to have task_id
        signature.freeze()
        node = self.add_signature(signature, dependencies)
        return [node]

    def add_celery_chain(self, chain, dependencies=None):
        """
        Consume celery chain.
        Chain is an ordered set of canvases e.g:
            c1 -> c2 -> c3 -> c4
        As a result:
            c1 depends on given `dependencies`
            c4 is the signature to depend on
        """
        # rely on celery internals to iterate over chain
        last_dependencies = dependencies
        for task in inspect.get_chain_tasks(chain):
            last_dependencies = self.add_celery_canvas(task, last_dependencies)

        return last_dependencies

    def add_celery_group(self, group, dependencies):
        """
        Consume celery group.
        Group is a set of tasks executed in parallel
            (c1, c2, c3, c4) - execute in parallel
        As a result:
            all tasks depend on given `dependencies`
            all tasks have to be dependent on
        """
        depend_on = []
        for task in inspect.get_group_tasks(group):
            task_dependencies = self.add_celery_canvas(task, dependencies)
            depend_on.extend(task_dependencies)

        # use previous dependencies if group is empty
        depend_on = depend_on or dependencies

        return depend_on

    def add_celery_chord(self, chord, dependencies):
        """
        Consume celery chord.
        Chord is a group of tasks executed in parallel followed by 'barrier'
        task e.g.:
            (c1, c2, c3, c4) -> c5
            c[1-4] - execute in parallel
            c5 - waits for c[1-4]
        As a result
            c[1-4] is called header and depends on `dependencies`
            c5 is called body and depends on c[1-4]
            c5 is the task to be dependent on
        """
        body, header = inspect.get_chord_tasks(chord)

        header_depend_on = []
        for task in header:
            task_dependencies = self.add_celery_canvas(task, dependencies)
            header_depend_on.extend(task_dependencies)

        # body needs to depend on dependencies given by header
        # or previous dependencies if header is empty
        header_depend_on = header_depend_on or dependencies

        depend_on = self.add_celery_canvas(body, header_depend_on)
        return depend_on


class WorkflowSignalMixin(object):
    hooks = {
        'after_active_tick': [],
        'on_finish': [],
        'on_state_change': [],
    }

    @classmethod
    def connect(cls, hook_name):
        """Wrapped function must accept two arguments:
            workflow - instance of workflow
            payload - payload passed when signal is emitted
        """
        if hook_name not in cls.hooks:
            raise WorkflowException("'{}': invalid signal".format(hook_name))

        def real_connect(function):
            cls.hooks[hook_name].append(function)
            return function

        return real_connect

    def emit(self, hook_name, payload=None):
        assert hook_name in self.hooks
        for hook in self.hooks[hook_name]:
            # hook handlers are optional and they should not break
            # an execution of a workflow
            try:
                hook(self, payload)
            except Exception as e:
                self.logger.warning(
                    "There was an exception during hook handler execution, "
                    "'%s' -> '%s' failed due to '%s'",
                    hook_name, hook, e
                )


class WorkflowState(object):
    # workflow in initial state
    INITIAL = 'INITIAL'
    # workflow in progress
    RUNNING = 'RUNNING'
    # workflow finished properly
    SUCCESS = 'SUCCESS'
    # workflow exec failed due to failed tasks
    FAILURE = 'FAILURE'
    # unexpected error occured
    ERROR = 'ERROR'

    # CANCEL/REVOKE is not supported here as its not handled
    # by dyrygent. If operation is revoked we don't get any info
    # so such state has to be handled elsewhere
    # WATITING is not supported now as we don't inspect task ETA for now
    # it might be implemented later


class WorkflowAlreadyRunningException(WorkflowException):
    """
    Internal exception raised when multiple workflow processors
    are detected
    """
    pass


class Workflow(WorkflowSignalMixin, CeleryWorkflowMixin):
    """
    Main class wrapping whole workflow, its signatures and inter-signature
    dependencies
    """
    workflow_processor_task = None

    # by default processing task will be retried for 7
    max_processing_time = int(timedelta(days=7).total_seconds())

    straight_serializables = {
        'running': {},
        'finished': {},
        'processing_limit_ts': None,
        'version': 1,
        'retry_policy': ['random', 3, 10],
        'id': None,
        'state': WorkflowState.INITIAL,
        'tasks_options': {},
        'workflow_options': {},
        'custom_payload': {}
    }

    def __init__(self, options=None):
        """
        Options are being transferred to the workflow task, as defined in
        https://docs.celeryproject.org/en/stable/reference/celery.app.task.html#celery.app.task.Task.apply_async
        """
        # holds state of workflow
        self.state = WorkflowState.INITIAL

        # dict holds all workflow nodes (wrapping signatures)
        self.nodes = {}

        # task is in running state if its id is in the dict
        self.running = {}

        # task finished successfuly or not
        self.finished = {}

        # holds workflow processor signature (after freeze())
        self._signature = None
        self.id = None

        # final execution timestamp, execution reaches it then processing task
        # will fail
        self.processing_limit_ts = None

        # version of workflow structure, might be useful to deserialize
        # workflows scheduled by older version of code
        self.version = 1
        self.set_retry_policy('random', 10, 30)

        # some stats/metrics holder
        self.stats = {
            'last_apply_async_tick': 0,
            'ticks': 0,
            'consecutive_celery_error_ticks': 0,
        }

        # internal var, whether tick scheduled some tasks or not
        # used for smarter retries
        self._active_tick = False
        # internal var, determines whether there were detected
        # celery erorrs within tick
        self._celery_errors_within_tick = []

        self.custom_payload = {}

        # options for apply_async
        self.tasks_options = {}
        self.workflow_options = options or {}

        # create instance level logger
        self.__init_logger()

    def add_signature(self, signature, dependencies=None):
        """
        Add signature to Workflow, returns created WorkflowNode instance
        """
        node = WorkflowNode(signature)
        dependencies = dependencies if dependencies is not None else []
        # if signature is already added then perhaps there is some bug in code
        if node.id in self.nodes:
            raise WorkflowException(
                "Looks like signature '{}' is already added to the workflow"
                .format(signature)
            )

        self.nodes[node.id] = node

        for required_node in dependencies:
            node.add_dependency(required_node)

        return node

    def get_tasks_state(self, task_ids):
        """
        Fetch task states from celery.
        Return dict {task_id: state}
        State is celery.result.AsyncResult instance
        """
        states = {
            task_id: inspect.get_task_state(task_id)
            for task_id in task_ids
        }
        return states

    def update_pending(self):
        """
        Move pending tasks to finished when task state is ready
        """
        # get finished tasks states
        states = self.get_tasks_state(self.running)

        for task_id, result in states.items():
            if not result.is_done():
                self.logger.debug("Task %s is still running", task_id)
                continue

            # mitigation for celery bug
            if not result.is_final():
                self.logger.warning(
                    "Task %s detected as ready but got exception '%s', "
                    "assuming it will be retried and executed",
                    task_id, str(result.value)
                )
                self._celery_errors_within_tick.append(
                    "Problem with task {}".format(task_id)
                )
                # usualy celery retries the task on its own
                # but there are some corner cases where it doesn't do it.
                # We wait for 2 ticks and reschedule the task.
                # This mechanism might interfere with evaluating task result
                # for a few times (both use the same counter) but lets
                # ignore that
                self.running[task_id] += 1
                if self.running[task_id] == 3:
                    self.reschedule_node_exec(self.nodes[task_id])

                continue

            # mitigation for celery bug
            if result.needs_reschedule():
                self.logger.warning(
                    "Task %s detected with exception '%s', "
                    "requires reschedule",
                    task_id, str(result.value)
                )
                self._celery_errors_within_tick.append(
                    "Reschedule task {}".format(task_id)
                )
                self.reschedule_node_exec(self.nodes[task_id])
                continue

            # mitigation for celery bug
            if not result.is_successful() and self.running[task_id] <= 3:
                self.running[task_id] += 1
                # Celery occasionally returns successful task as not
                # successful. Check failed state up to 3 times before
                # treating the fail as permanent
                continue

            if self.running[task_id] and self.running[task_id] > 1:
                self.logger.info(
                    "Task %s has final state after %s checks",
                    task_id, self.running[task_id]
                )
            del self.running[task_id]
            self.finished[task_id] = result.is_successful()

            if self.finished[task_id]:
                self.logger.info("Task %s is done, success", task_id)
            else:
                self.logger.info(
                    "Task %s is done, failure, result '%s(%s)'",
                    task_id, type(result.value), result.value
                )

    def get_unlocked_dependencies(self):
        """
        Get set of tasks which are ready with state success
        """
        return set([
            task_id
            for task_id, task_success in self.finished.items()
            if task_success
        ])

    def get_signatures_to_run(self):
        """
        Find signatures which should be executed according to dependencies
        """
        ok_tasks = self.get_unlocked_dependencies()
        to_run = []

        for node_id, node in self.nodes.items():
            if node_id in self.running or node_id in self.finished:
                continue

            if set(node.dependencies).issubset(ok_tasks):
                to_run.append(node)
                self.logger.debug(
                    "Workflow node %s dependencies fulfilled",
                    node.id
                )
        return to_run

    def are_scheduled(self, nodes_to_run):
        """
        Check if task to be scheduled is already scheduled.
        Return task states if scheduled
        """
        # if there is more than one instance of workflow processor running
        # additional ones should finish immediately
        # as only one of them is needed to push the workflow forward.
        # In most cases such situation can be detected when task which is
        # about to be scheduled is already running.
        states = self.get_tasks_state([n.id for n in nodes_to_run])
        return {
            _id: state.task_state
            for _id, state in states.items()
            if state.was_published() and not state.needs_reschedule()
        }

    def schedule_node_exec(self, node):
        assert node.id not in self.running
        self.logger.info("Scheduling execution of task %s with options %s",
                         node.id,
                         self.tasks_options)
        node.signature.apply_async(**self.tasks_options)

        # store number here, the number will be used as counter of
        # consecutive checks for failed tasks in order to mitigate
        # celery issue reporting successful tasks as failed.
        # Task failure will be considered permanent after 3 state evaluations
        self.running[node.id] = 1

    def reschedule_node_exec(self, node):
        assert node.id in self.running
        self.logger.info("Rescheduling execution of task %s with options %s",
                         node.id,
                         self.tasks_options)
        node.signature.apply_async(**self.tasks_options)

    def tick(self):
        try:
            self.set_state(WorkflowState.RUNNING)
            running = self._tick()

        except WorkflowAlreadyRunningException:
            self.logger.warning(
                "Detected multiple workflow processors '%s', "
                "this instance stops execution now",
                self.id
            )
            # this particular instance should no longer run
            return False

        except Exception as e:
            self.set_state(WorkflowState.ERROR)
            self.logger.exception(e)
            raise

        if not running:
            if self.is_success():
                self.set_state(WorkflowState.SUCCESS)
            else:
                self.set_state(WorkflowState.FAILURE)
        return running

    def _tick(self):
        """
        Update internal task states
        Execute tasks if dependencies are met
        Return true if there is anything to run
        """
        self.stats['ticks'] += 1
        ts = time.time()
        self.check_processing_time()
        self.update_pending()

        sigs_to_run = self.get_signatures_to_run()

        already_scheduled = self.are_scheduled(sigs_to_run)
        if already_scheduled:
            self.logger.warning(
                "Tasks %s already scheduled", already_scheduled
            )
            raise WorkflowAlreadyRunningException(self.id)

        for node in sigs_to_run:
            self.schedule_node_exec(node)

        self._active_tick = False
        if sigs_to_run:
            self.stats['last_apply_async_tick'] = int(time.time())
            self._active_tick = True

        self.check_consecutive_fails()
        if self.running:
            self.logger.info(
                "Tick done, took %fms, workflow running, waiting for %s tasks",
                time.time() - ts, len(self.running)
            )
            if self._active_tick:
                self.emit('after_active_tick')
            return True

        else:
            self.logger.info(
                "Tick done, took %fms, workflow finished after %s ticks",
                time.time() - ts, self.stats['ticks']
            )
            self.emit('on_finish')

    def is_success(self):
        return (
            set(self.nodes.keys()) == set(self.finished.keys()) and
            all(self.finished.values())
        )

    def set_state(self, state):
        if state != self.state:
            self.state = state
            self.emit('on_state_change', state)

    def iterate_dependencies(self):
        ordered_nodes = sorted(
            self.nodes.values(), key=lambda e: e.id
        )
        for node in ordered_nodes:
            for dep in sorted(node.dependencies.keys()):
                yield node, self.nodes[dep]
            if not node.dependencies:
                yield node, None

    def simulate_tick(self):
        """
        Simulate single tick, tasks from previous tick are considered done
        """
        for task_id in self.running:
            self.finished[task_id] = True
        self.running = {}

        for node in self.get_signatures_to_run():
            self.running[node.id] = True

        if self.running:
            return True

    def to_dict(self):
        res = dict(
            nodes={
                node.id: node.to_dict()
                for node in self.nodes.values()
            }
        )
        for attr in self.straight_serializables:
            res[attr] = getattr(self, attr)
        res['stats'] = self.stats
        res['custom_payload'] = self.custom_payload

        return res

    @classmethod
    def from_dict(cls, workflow_dict):
        obj = cls()
        obj.nodes = {
            node_id: WorkflowNode.from_dict(node_dict)
            for node_id, node_dict in workflow_dict['nodes'].items()
        }

        for attr, default in cls.straight_serializables.items():
            setattr(obj, attr, workflow_dict.get(attr, default))

        # need to treat stats in special way as stats may e.g.
        # have additional keys when version is updated
        obj.stats.update(workflow_dict['stats'])

        # workflow id was just set so reinitialize the logger
        obj.__init_logger()
        return obj

    @classmethod
    def set_workflow_processor_task(cls, task):
        cls.workflow_processor_task = task

    def set_max_processing_time(self, seconds):
        self.max_processing_time = seconds

    def evaluate_processing_limit(self):
        self.processing_limit_ts = (
            int(time.time()) + self.max_processing_time
        )

    def check_processing_time(self):
        if time.time() > self.processing_limit_ts:
            raise WorkflowException("Workflow is processed for too long")

    def set_retry_policy(self, retry_type, *retry_args):
        if retry_type != 'random':
            raise WorkflowException(
                'Invalid retry policy, got "{}"'
                .format(retry_type)
            )

        self.retry_policy = [retry_type]
        self.retry_policy.extend(retry_args)

    def get_retry_countdown(self):
        """
        Get retry countdown which will be used during processor task retry
        """

        func, random_min, random_max = self.retry_policy
        assert func == 'random'
        assert 0 < random_min <= random_max
        val = random.randint(random_min, random_max)

        # if previous tick
        active_tick_countdown = 2
        if self._active_tick and val > active_tick_countdown:
            val = active_tick_countdown

        return val

    def freeze(self):
        """
        Create task processing signature and freeze it.
        Internal state is transfomed into a dict and passed as a signature arg
        """
        if self.workflow_processor_task is None:
            raise WorkflowException(
                "Can't execute workflow, processing task is not set."
                " Please register the task using "
                "`register_workflow_processor` method."
            )
        self.evaluate_processing_limit()
        signature = self.workflow_processor_task.subtask(
            kwargs=dict(workflow_dict=self.to_dict())
        )
        signature.freeze()
        self._signature = signature
        self.id = self._signature.id
        self.__init_logger()
        self.logger.info("Workflow signature ready")
        return signature

    def apply_async(self, **kwargs):
        """
        Schedules execution of the workflow.
        accepts the same options as are defined in:
        https://docs.celeryproject.org/en/stable/reference/celery.app.task.html#celery.app.task.Task.apply_async
        and applies them to each individual celery task other than the workflow task.
        """
        self.tasks_options = kwargs
        if self._signature is None:
            self.freeze()

        self.logger.info("Scheduling workflow execution with options %s", self.workflow_options)
        return self._signature.apply_async(**self.workflow_options)

    def skip_tasks(self, task_ids):
        """
        Mark task_ids as finished. This function expects complete set
        of tasks that should be skipped. It does NOT take into account
        actual state of workflow (self.finished).
        """
        # verify that all nodes_to_skip have all dependencies finished
        nodes_to_skip = [self.nodes[node_id] for node_id in task_ids]

        # TODO verify if this works as desired,
        # it seems like we're checking if task can be skipped one by one
        # while this may not work when e.g:
        # - first task to skip depends on second task to skip
        # It seems that verification should be done when all tasks are skipped
        # this function should not take order of skips under consideration
        for node in nodes_to_skip:
            for parent_task_id in node.dependencies:
                if parent_task_id not in task_ids:
                    raise WorkflowException(
                        "Can't mark dependant task as finished when "
                        "not all parents finished")
        for task_id in task_ids:
            self.finished[task_id] = True

    def __init_logger(self):
        """
        Create instance of object level logger with workflow ID
        """
        self.logger = logging.getLogger('{}.{}'.format(__name__, self.id))

    def check_consecutive_fails(self):
        """
        There are some workarounds in dyrygent mitigating celery bugs.
        If These mitigations occur consecutive error counter is increased.
        Veirfy if consecutive fails reach some threshold, if yes then break
        the execution.
        """
        MAX_ERRORS = 10
        if self._celery_errors_within_tick:
            self.stats['consecutive_celery_error_ticks'] += 1
        else:
            self.stats['consecutive_celery_error_ticks'] = 0

        current_errors = self.stats['consecutive_celery_error_ticks']
        if current_errors >= MAX_ERRORS:
            self.logger.error(
                "Detected errors '%s'",
                self._celery_errors_within_tick
            )
            raise WorkflowException(
                "Max consecutive celery errors reached ({})"
                .format(MAX_ERRORS)
            )
