# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

import mock
import pytest

from celery_dyrygent.celery import (
    entities,
    inspect,
)
from celery_dyrygent.workflows import (
    Workflow,
    WorkflowException,
    WorkflowNode,
)


@pytest.fixture
def clear_wf_hooks():
    saved_hooks = Workflow.hooks
    # a bit stupid, have to add new signals here too
    # but can't find any other simple solution
    Workflow.hooks = {
        'after_active_tick': [],
        'on_finish': [],
        'on_state_change': [],
    }
    yield
    Workflow.hooks = saved_hooks


@pytest.fixture
def sig_factory():

    class Factory(object):
        current_id = 0

        def pop_id(self):
            self.current_id += 1
            return self.current_id

        def create(self, freeze=True):
            m = mock.Mock(spec=entities.Signature)
            m.id = None

            def mocked_freeze():
                if m.id is None:
                    m.id = 'task-{}'.format(self.pop_id())

            m.freeze.side_effect = mocked_freeze

            if freeze:
                m.freeze()

            return m

    return Factory()


class TestWorkflowNode(object):
    def test_constructor(self, sig_factory):
        sig = sig_factory.create()
        sig.freeze()

        node = WorkflowNode(sig)
        assert node.signature == sig
        assert node.dependencies == {}
        assert node.id == sig.id == 'task-1'
        sig.freeze.asset_called_once()

    def test_constructor_not_frozen(self, sig_factory):
        sig = sig_factory.create(freeze=False)
        with pytest.raises(WorkflowException, match='not frozen'):
            WorkflowNode(sig)

    def test_add_dependency(self, sig_factory):
        s1 = sig_factory.create()
        s2 = sig_factory.create()
        n1 = WorkflowNode(s1)
        n2 = WorkflowNode(s2)

        n1.add_dependency(n2)
        assert n1.dependencies
        assert n2.id in n1.dependencies

    def test_add_dependency_already_added(self, sig_factory):
        s1 = sig_factory.create()
        s2 = sig_factory.create()
        n1 = WorkflowNode(s1)
        n2 = WorkflowNode(s2)
        n1.add_dependency(n2)
        with pytest.raises(WorkflowException, match='already added'):
            n1.add_dependency(n2)

    def test_to_dict(self, sig_factory):
        w1 = WorkflowNode(sig_factory.create())
        w2 = WorkflowNode(sig_factory.create())
        w3 = WorkflowNode(sig_factory.create())

        w1.add_dependency(w2)
        w1.add_dependency(w3)

        d = w1.to_dict()

        assert d['id'] == 'task-1'
        assert d['dependencies']
        assert d['dependencies']['task-2'] is None
        assert d['dependencies']['task-3'] is None
        assert d['signature'] is w1.signature

    def test_from_dict(self):
        data_dict = {
            'id': 'task-7',
            'signature': {
                'options': {'task_id': 'task-7'}, 'task': None,
                'args': (), 'kwargs': {},
            },
            'dependencies': {'task-4': None},
        }
        w1 = WorkflowNode.from_dict(data_dict)

        assert w1.id == 'task-7'
        assert w1.signature.id == 'task-7'
        assert w1.dependencies == {'task-4': None}


class TestWorkflow(object):
    @pytest.fixture
    def wf(self):
        return Workflow()

    def test_constructor(self):
        wf = Workflow()
        assert wf.nodes == {}

    def test_add_signature(self, wf, sig_factory):
        sig = sig_factory.create()
        res = wf.add_signature(signature=sig)

        assert isinstance(res, WorkflowNode)
        assert res.id == 'task-1'
        assert res.id in wf.nodes
        assert wf.nodes[res.id] is res

    def test_add_signature_with_dependencies(self, wf, sig_factory):
        sigs = [sig_factory.create() for i in range(3)]

        n1 = wf.add_signature(signature=sigs[0])
        n2 = wf.add_signature(signature=sigs[1], dependencies=[n1])
        n3 = wf.add_signature(signature=sigs[2], dependencies=[n1, n2])

        assert n1.dependencies == {}
        assert n1.id in n2.dependencies
        assert n1.id in n3.dependencies
        assert n2.id in n3.dependencies

    def test_update_pending(self, wf, sig_factory):
        sigs = [sig_factory.create() for i in range(3)]

        wf.running[sigs[0].id] = None
        wf.running[sigs[1].id] = None

        with mock.patch.object(inspect, 'get_task_state') as gts:
            task1 = mock.Mock()
            task2 = mock.Mock()
            results = {
                'task-1': task1,
                'task-2': task2,
            }

            def side_effect(task_id):
                return results[task_id]

            gts.side_effect = side_effect

            task1.is_done.return_value = False
            task1.is_final.return_value = False
            task1.needs_reschedule.return_value = False
            task2.is_done.return_value = False
            task2.is_final.return_value = False
            task2.needs_reschedule.return_value = False
            wf.update_pending()

            assert wf.finished == {}
            assert 'task-1' in wf.running
            assert 'task-2' in wf.running

            task1.is_done.return_value = True
            task1.is_final.return_value = True

            wf.update_pending()
            assert 'task-1' not in wf.running
            assert 'task-2' in wf.running
            assert 'task-1' in wf.finished
            assert not wf._celery_errors_within_tick

    @mock.patch.object(inspect, 'get_task_state')
    @mock.patch.object(Workflow, 'reschedule_node_exec')
    def test_update_pending_non_final(self, resched, gts, wf, sig_factory):
        sigs = [sig_factory.create() for i in range(2)]

        n1 = wf.add_celery_signature(sigs[0])[0]
        n2 = wf.add_celery_signature(sigs[1])[0]
        wf.schedule_node_exec(n1)
        wf.schedule_node_exec(n2)

        task1 = mock.Mock()
        task2 = mock.Mock()
        results = {
            'task-1': task1,
            'task-2': task2,
        }

        def side_effect(task_id):
            return results[task_id]

        gts.side_effect = side_effect

        task1.is_done.return_value = True
        task1.is_final.return_value = True
        task1.needs_reschedule.return_value = False
        task2.is_done.return_value = True
        task2.is_final.return_value = False
        task2.needs_reschedule.return_value = False

        wf.update_pending()
        assert 'task-1' not in wf.running
        assert 'task-2' in wf.running
        assert 'task-1' in wf.finished
        assert 'task-2' not in wf.finished
        assert wf._celery_errors_within_tick
        resched.assert_not_called()

        wf.update_pending()
        resched.assert_called_once()
        resched.assert_called_with(n2)

    @mock.patch.object(inspect, 'get_task_state')
    @mock.patch.object(Workflow, 'reschedule_node_exec')
    def test_update_pending_needs_reschedule(
        self, resched, gts, wf, sig_factory
    ):
        sigs = [sig_factory.create() for i in range(2)]

        n1 = wf.add_celery_signature(sigs[0])[0]
        n2 = wf.add_celery_signature(sigs[1])[0]
        wf.schedule_node_exec(n1)
        wf.schedule_node_exec(n2)

        task1 = mock.Mock()
        task2 = mock.Mock()
        results = {
            'task-1': task1,
            'task-2': task2,
        }

        def side_effect(task_id):
            return results[task_id]

        gts.side_effect = side_effect

        task1.is_done.return_value = True
        task1.is_final.return_value = True
        task1.needs_reschedule.return_value = False
        task2.is_done.return_value = True
        task2.is_final.return_value = True
        task2.needs_reschedule.return_value = True

        wf.update_pending()
        resched.assert_called_once()
        resched.assert_called_with(n2)
        assert 'task-1' not in wf.running
        assert 'task-2' in wf.running
        assert 'task-1' in wf.finished
        assert 'task-2' not in wf.finished
        assert wf._celery_errors_within_tick

    def test_update_pending_failed_task(self, wf, sig_factory):
        sig = sig_factory.create()
        wf.running[sig.id] = 1

        with mock.patch.object(inspect, 'get_task_state') as gts:
            task1 = mock.Mock()
            results = {'task-1': task1, }

            def side_effect(task_id):
                return results[task_id]

            gts.side_effect = side_effect

            task1.is_done.return_value = True
            task1.is_final.return_value = True
            task1.needs_reschedule.return_value = False
            task1.is_successful.return_value = False

            wf.update_pending()
            assert wf.running['task-1'] == 2
            assert 'task-1' not in wf.finished

            wf.update_pending()
            assert wf.running['task-1'] == 3
            assert 'task-1' not in wf.finished

            wf.update_pending()
            assert wf.running['task-1'] == 4
            assert 'task-1' not in wf.finished

            wf.update_pending()
            assert 'task-1' not in wf.running
            assert 'task-1' in wf.finished

    def test_get_tasks_state(self, wf):
        with mock.patch.object(inspect, 'get_task_state') as gts:
            results = {
                i: mock.Mock()
                for i in range(4)
            }

            def side_effect(task_id):
                return results[task_id]

            gts.side_effect = side_effect
            res = wf.get_tasks_state([1, 2, 3])

            gts.assert_has_calls(
                [mock.call(1), mock.call(2), mock.call(3)]
            )

            assert res[1] == results[1]
            assert res[2] == results[2]
            assert res[3] == results[3]
            assert len(res) == 3

    def test_get_unlocked_dependencies(self, wf):
        wf.finished = {
            1: False,
            2: True,
            3: False,
            4: True,
            5: True
        }

        res = wf.get_unlocked_dependencies()
        assert res == set([2, 4, 5])

    def test_are_scheduled(self, wf):
        with mock.patch.object(Workflow, 'get_tasks_state') as gts:
            nodes = {i: mock.Mock() for i in range(4)}

            for node_id, result in list(nodes.items()):
                result.id = node_id
                result.was_published.return_value = False
                result.needs_reschedule.return_value = False
                result.task_state = 'PENDING'

            gts.return_value = nodes

            res = wf.are_scheduled(nodes.values())
            assert res == {}

            nodes[1].was_published.return_value = True
            nodes[3].was_published.return_value = True

            res = wf.are_scheduled(nodes.values())
            assert res == {
                1: nodes[1].task_state,
                3: nodes[2].task_state,
            }

            nodes[3].needs_reschedule.return_value = True

            res = wf.are_scheduled(nodes.values())
            assert res == {
                1: nodes[1].task_state,
            }

    @mock.patch.object(Workflow, 'get_unlocked_dependencies')
    def test_get_signatures_to_run(self, mck, wf, sig_factory):
        sigs = [sig_factory.create() for i in range(10)]

        n1 = wf.add_signature(sigs[0])
        n2 = wf.add_signature(sigs[1])
        n3 = wf.add_signature(sigs[2])
        n4 = wf.add_signature(sigs[3])
        mck.return_value = set()

        n2.add_dependency(n1)
        n3.add_dependency(n2)
        res = wf.get_signatures_to_run()
        assert set(res) == set([n1, n4])

        wf.finished[n4.id] = True
        res = wf.get_signatures_to_run()
        assert set(res) == set([n1])

        wf.finished[n1.id] = True
        res = wf.get_signatures_to_run()
        assert set(res) == set()

        mck.return_value = set([n1.id])
        res = wf.get_signatures_to_run()
        assert set(res) == set([n2])

        mck.return_value = set([n2.id])
        res = wf.get_signatures_to_run()
        assert set(res) == set([n3])

    @mock.patch.object(Workflow, 'update_pending')
    @mock.patch.object(Workflow, 'get_signatures_to_run')
    @mock.patch.object(Workflow, 'are_scheduled')
    def test_tick(self, are_sched, gs, upd_pend, wf):
        are_sched.return_value = {}
        wf.set_workflow_processor_task(mock.Mock())
        wf.freeze()
        res = wf.tick()
        upd_pend.assert_called_with()
        gs.assert_called_with()

        # nothing still running so returns false/none
        assert not res
        assert wf.running == {}

        sig1 = mock.Mock()
        sig2 = mock.Mock()
        gs.return_value = [sig1, sig2]
        res = wf.tick()

        assert res is True
        assert wf.running[sig1.id] == 1
        assert wf.running[sig2.id] == 1

    @mock.patch.object(Workflow, 'update_pending')
    @mock.patch.object(Workflow, 'get_signatures_to_run')
    @mock.patch.object(Workflow, 'are_scheduled')
    def test_tick_already_scheduled(self, are_sched, gs, upd_pend, wf):
        wf.set_workflow_processor_task(mock.Mock())
        wf.freeze()

        sig1 = mock.Mock()
        sig2 = mock.Mock()
        gs.return_value = [sig1, sig2]
        are_sched.return_value = {sig1.id: 'RUNNING'}
        res = wf.tick()
        upd_pend.assert_called_with()

        assert res is False
        assert sig1.id not in wf.running
        assert sig2.id not in wf.running

    @mock.patch.object(Workflow, 'update_pending')
    @mock.patch.object(Workflow, 'get_signatures_to_run')
    @mock.patch.object(Workflow, 'are_scheduled')
    def test_tick_emit_after_active_tick(
        self, are_sched, gs, upd_pend, wf, clear_wf_hooks
    ):
        are_sched.return_value = {}
        wf.set_workflow_processor_task(mock.Mock())
        wf.freeze()
        handle_after_active_tick = mock.Mock()
        handle_finish = mock.Mock()

        wf.connect('after_active_tick')(handle_after_active_tick)
        wf.connect('on_finish')(handle_finish)
        gs.return_value = [mock.Mock()]
        wf.tick()
        handle_after_active_tick.assert_called_with(wf, None)
        handle_finish.assert_not_called()

        gs.return_value = []
        wf.tick()
        handle_after_active_tick.assert_called_once()
        handle_finish.assert_not_called()

    @mock.patch.object(Workflow, 'update_pending')
    @mock.patch.object(Workflow, 'get_signatures_to_run')
    def test_tick_emit_finish(self, gs, upd_pend, wf):
        wf.set_workflow_processor_task(mock.Mock())
        wf.freeze()
        handle_after_active_tick = mock.Mock()
        handle_finish = mock.Mock()
        wf.connect('after_active_tick')(handle_after_active_tick)
        wf.connect('on_finish')(handle_finish)
        gs.return_value = []
        wf.tick()
        handle_after_active_tick.assert_not_called()
        handle_finish.assert_called_with(wf, None)

    @mock.patch.object(Workflow, 'get_signatures_to_run')
    def test_tick_stats(self, gs, wf):
        wf.set_workflow_processor_task(mock.Mock())
        wf.freeze()

        gs.return_value = []
        wf.tick()

        assert wf.stats['last_apply_async_tick'] == 0

        gs.return_value = [mock.Mock()]
        with mock.patch('celery_dyrygent.workflows.workflow.time') as time:
            with mock.patch.object(Workflow, 'are_scheduled') as are_sched:
                are_sched.return_value = {}
                time.time.return_value = 777
                wf.tick()
                assert wf.stats['last_apply_async_tick'] == 777

    def test_tick_too_late(self, wf):
        wf.set_max_processing_time(0)
        wf.freeze()
        with pytest.raises(WorkflowException, match='for too long'):
            wf.tick()

    def test_to_dict(self, wf):
        wf.running = {
            '1': True,
            '2': False,
        }
        wf.finished = {
            '6': True,
            '7': False,
        }
        wf.nodes['10'] = mock.Mock()
        wf.nodes['10'].id = '10'
        wf.nodes['10'].to_dict.return_value = 'inner'
        wf.processing_limit_ts = 500

        res = wf.to_dict()

        assert res == {
            'running': {
                '1': True,
                '2': False,
            },
            'finished': {
                '6': True,
                '7': False,
            },
            'nodes': {
                '10': 'inner',
            },
            'processing_limit_ts': 500,
            'version': 1,
            'retry_policy': ['random', 10, 30],
            'stats': {
                'last_apply_async_tick': 0,
                'ticks': 0,
                'consecutive_celery_error_ticks': 0,
            },
            'id': None,
            'state': 'INITIAL',
            'tasks_options': {},
            'workflow_options': {}
        }

        wf.nodes['10'].to_dict.assert_called()

    def test_from_dict(self):
        wf_dict = {
            'finished': {'1': False},
            'running': {'2': True},
            'nodes': {'some': 'data', 'some2': 'data2'},
            'processing_limit_ts': 5000,
            'version': 1,
            'retry_policy': ['random', 10, 30],
            'stats': {
                'last_apply_async_tick': 0,
                'ticks': 25
            },
            'id': None,
            'state': 'RUNNING',
            'tasks_options': {},
            'workflow_options': {}
        }
        with mock.patch.object(WorkflowNode, 'from_dict') as mck:
            mck.return_value = 'some_result'
            wf = Workflow.from_dict(wf_dict)
            assert wf.state == 'RUNNING'
            assert wf.running == {'2': True}
            assert wf.finished == {'1': False}
            assert wf.version == 1
            assert wf.nodes == {
                'some': 'some_result',
                'some2': 'some_result',
            }
            assert wf.stats == {
                'last_apply_async_tick': 0,
                'ticks': 25,
                'consecutive_celery_error_ticks': 0,
            }

            assert wf.processing_limit_ts == 5000

            mck.assert_has_calls([
                mock.call('data'),
                mock.call('data2'),
            ])

    def test_freeze_no_task(self):
        wf = Workflow()
        wf.set_workflow_processor_task(None)
        with pytest.raises(WorkflowException, match="task is not set"):
            wf.freeze()

    def test_processing_limit(self):
        wf = Workflow()
        assert wf.max_processing_time == 604800

        wf.set_max_processing_time(1200)
        assert wf.max_processing_time == 1200

        wf2 = Workflow()
        assert wf2.max_processing_time == 604800

    def test_evaluate_processing_limit(self):
        wf = Workflow()
        wf.set_max_processing_time(1200)
        with mock.patch('celery_dyrygent.workflows.workflow.time') as mck:
            mck.time.return_value = 100
            wf.evaluate_processing_limit()
            wf.processing_limit_ts == 1300

    def test_apply_async(self):
        wf = Workflow()
        task_cls = mock.Mock()
        signature = mock.Mock()
        task_cls.subtask.return_value = signature
        signature.id = 'dummy'
        Workflow.set_workflow_processor_task(task_cls)

        wf.apply_async()
        assert wf.id == 'dummy'

        # trickery, task_cls.subtask was called before ID was assigned
        # to workflow, while now wf.to_dict has id field set
        # just set it back to None
        wf.id = None
        task_cls.subtask.assert_called_with(
            kwargs=dict(workflow_dict=wf.to_dict())
        )
        signature.apply_async.assert_called()

    def test_apply_async_uses_workflow_options(self):
        options = {'option': True}
        wf = Workflow(options=options)
        task_cls = mock.Mock()
        signature = mock.Mock()
        task_cls.subtask.return_value = signature
        signature.id = 'dummy'
        Workflow.set_workflow_processor_task(task_cls)

        wf.apply_async()
        assert wf.id == 'dummy'

        # trickery, task_cls.subtask was called before ID was assigned
        # to workflow, while now wf.to_dict has id field set
        # just set it back to None
        wf.id = None
        task_cls.subtask.assert_called_with(
            kwargs=dict(workflow_dict=wf.to_dict())
        )
        signature.apply_async.assert_called_with(**options)

    def test_schedule_node_exec_uses_task_options(self):
        wf = Workflow()
        task_cls = mock.Mock()
        signature = mock.Mock()
        node = mock.Mock()
        node.id = 'task_node'
        task_cls.subtask.return_value = signature
        signature.id = 'dummy'
        Workflow.set_workflow_processor_task(task_cls)

        options = {'option': True}
        wf.apply_async(options=options)
        wf.schedule_node_exec(node)

        signature.apply_async.assert_called_with()
        node.signature.apply_async.assert_called_with(**options)

    def test_check_processing_time(self):
        wf = Workflow()
        wf.set_max_processing_time(0)
        wf.evaluate_processing_limit()
        with pytest.raises(WorkflowException, match='for too long'):
            wf.check_processing_time()

    def test_set_retry_policy_invalid(self):
        wf = Workflow()

        with pytest.raises(WorkflowException, match='Invalid retry policy'):
            wf.set_retry_policy('dummy')

    def test_set_retry_policy(self):
        wf = Workflow()
        wf.set_retry_policy('random', 30, 60)
        assert wf.retry_policy == ['random', 30, 60]

    def test_get_retry_countdown(self):
        wf = Workflow()
        with mock.patch('celery_dyrygent.workflows.workflow.random') as mck:
            mck.randint.return_value = 777
            res = wf.get_retry_countdown()

            mck.randint.assert_called_with(10, 30)

            assert res == 777

            wf._active_tick = True
            assert wf.get_retry_countdown() == 2

    def test_connect_invalid_signal(self, clear_wf_hooks):
        with pytest.raises(WorkflowException, match=r'invalid signal'):
            Workflow.connect('invalid')(mock.Mock())

    def test_connect_chaining(self, clear_wf_hooks):
        hook = mock.Mock()
        Workflow.connect('after_active_tick')(
            Workflow.connect('on_finish')(hook)
        )
        assert Workflow.hooks['after_active_tick'] == [hook]
        assert Workflow.hooks['on_finish'] == [hook]

    def test_skip_tasks(self, wf, sig_factory):
        sigs = [sig_factory.create() for i in range(3)]

        n1 = wf.add_signature(signature=sigs[0])
        n2 = wf.add_signature(signature=sigs[1], dependencies=[n1])
        n3 = wf.add_signature(signature=sigs[2], dependencies=[n1, n2])

        wf.skip_tasks(task_ids=[n1.id])
        wf.skip_tasks(task_ids=[n1.id, n2.id])
        wf.skip_tasks(task_ids=[n1.id, n2.id, n3.id])

    def test_skip_tasks_broken_dependency(self, wf, sig_factory):
        sigs = [sig_factory.create() for i in range(4)]

        n1 = wf.add_signature(signature=sigs[0])
        n2 = wf.add_signature(signature=sigs[1], dependencies=[n1])
        n3 = wf.add_signature(signature=sigs[2], dependencies=[n1, n2])
        n4 = wf.add_signature(signature=sigs[3], dependencies=[n3])

        for deps in [[n2.id], [n1.id, n3.id], [n4.id]]:
            with pytest.raises(WorkflowException, match='mark dependant task '
                               'as finished when not all parents finished'):
                wf.skip_tasks(task_ids=deps)

    @pytest.mark.parametrize('current, error_tick, expected', [
        (0, False, 0),
        (0, True, 1),
        (5, True, 6),
        (8, False, 0),
    ])
    def test_check_consecutive_fails_stats(
        self, wf, current, error_tick, expected
    ):
        wf._celery_errors_within_tick = error_tick
        wf.stats['consecutive_celery_error_ticks'] = current
        wf.check_consecutive_fails()
        assert wf.stats['consecutive_celery_error_ticks'] == expected

    def test_check_consecutive_fails_exceeded(self, wf):
        wf._celery_errors_within_tick = [True]
        wf.stats['consecutive_celery_error_ticks'] = 9
        with pytest.raises(WorkflowException, match='Max consecutive'):
            wf.check_consecutive_fails()

    def test_set_state(self, wf, clear_wf_hooks):
        handler = mock.Mock()
        Workflow.connect('on_state_change')(handler)

        wf.set_state('INITIAL')
        handler.assert_not_called()

        wf.set_state('DUMMY')
        handler.assert_called_with(wf, 'DUMMY')

    def test_is_success(self, wf, sig_factory):
        sigs = [sig_factory.create() for i in range(10)]

        n1 = wf.add_signature(sigs[0])
        n2 = wf.add_signature(sigs[1])

        wf.finished[n1.id] = True
        wf.finished[n2.id] = True

        assert wf.is_success() is True

        wf.finished[n2.id] = False
        assert wf.is_success() is False

        del wf.finished[n2.id]
        assert wf.is_success() is False

        del wf.nodes[n2.id]
        assert wf.is_success() is True

    @mock.patch.object(Workflow, '_tick')
    @mock.patch.object(Workflow, 'set_state')
    def test_tick_running_set_state(self, _tick, set_state, wf):
        wf.tick()
        wf.set_state.assert_called_with('RUNNING')

        wf._tick.side_effect = Exception('test')
        with pytest.raises(Exception, match='test'):
            wf.tick()

        wf.set_state.assert_called_with('ERROR')

    @mock.patch.object(Workflow, '_tick')
    @mock.patch.object(Workflow, 'set_state')
    @mock.patch.object(Workflow, 'is_success')
    def test_tick_not_running_set_state(
        self, _tick, set_state, is_success, wf
    ):
        wf._tick.return_value = False
        wf.is_success.return_value = True
        wf.tick()
        wf.set_state.assert_called_with('SUCCESS')

        wf.is_success.return_value = False
        wf.tick()
        wf.set_state.assert_called_with('FAILURE')

    @mock.patch.object(Workflow, 'update_pending')
    @mock.patch.object(Workflow, 'get_signatures_to_run')
    @mock.patch.object(Workflow, 'are_scheduled')
    @mock.patch.object(Workflow, 'check_processing_time')
    @mock.patch.object(Workflow, 'is_success')
    def test_multiple_processors_doesnt_set_error_state(
        self, wf_success, proc_time, are_sched, gs, upd_pend, wf
    ):
        are_sched.return_value = True
        wf_success.return_value = False
        running = wf.tick()

        assert wf.state == 'RUNNING'
        assert running is False
