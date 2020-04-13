# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

import celery
import pytest
from distutils.version import LooseVersion
from celery import canvas as celery_canvas

from celery_dyrygent import (
    tasks,
)
from celery_dyrygent.celery import (
    entities,
)
from celery_dyrygent.workflows import (
    Workflow,
)

# perhaps its enough to test functions from celery.inspect module


def build_dependency_asserts(wf):
    for node, dependency in wf.iterate_dependencies():
        if dependency is None:
            print(
                'assert wf.nodes["{}"].dependencies == {{}}  # noqa'
                .format(node.id)
            )
            continue

        print(
            'assert "{}" in wf.nodes["{}"].dependencies  # noqa'
            .format(dependency.id, node.id)
        )


def build_exec_asserts(wf):
    while wf.simulate_tick():
        print('assert wf.simulate_tick()')
        print('assert wf.running == {}  # noqa'.format(wf.running))
    print('assert not wf.simulate_tick()')


class TestWorkflows(object):
    @pytest.fixture
    def app(self):
        res = entities.Celery()
        tasks.register_workflow_processor(res)
        return res

    @pytest.fixture
    def some_sigs(self):
        sigs = [
            celery_canvas.Signature(task_id='task-{}'.format(r))
            for r in range(10)
        ]

        for sig in sigs:
            sig.freeze()
        return sigs

    def test_add_celery_canvas(self, some_sigs):
        wf = Workflow()
        c1 = some_sigs[0] | some_sigs[1]
        wf.add_celery_chain(c1)

        assert some_sigs[0].id in wf.nodes
        assert some_sigs[1].id in wf.nodes

    def test_add_celery_signature(self):
        wf = Workflow()
        sig = celery_canvas.Signature()
        res = wf.add_celery_signature(sig)
        node = res[0]
        assert node.id == sig.id

    def test_add_celery_chain(self, some_sigs):
        wf = Workflow()
        sigs = some_sigs

        chain = celery_canvas.chain(sigs[:-2])
        dependencies = [
            wf.add_signature(sigs[-2]),
            wf.add_signature(sigs[-1]),
        ]
        res = wf.add_celery_chain(chain, dependencies)

        assert res == [wf.nodes[sigs[-3].id]]

        # use build_dependency_asserts(wf) to regenerate
        # build_dependency_asserts(wf)
        assert "task-8" in wf.nodes["task-0"].dependencies  # noqa
        assert "task-9" in wf.nodes["task-0"].dependencies  # noqa
        assert "task-0" in wf.nodes["task-1"].dependencies  # noqa
        assert "task-1" in wf.nodes["task-2"].dependencies  # noqa
        assert "task-2" in wf.nodes["task-3"].dependencies  # noqa
        assert "task-3" in wf.nodes["task-4"].dependencies  # noqa
        assert "task-4" in wf.nodes["task-5"].dependencies  # noqa
        assert "task-5" in wf.nodes["task-6"].dependencies  # noqa
        assert "task-6" in wf.nodes["task-7"].dependencies  # noqa
        assert wf.nodes["task-8"].dependencies == {}  # noqa
        assert wf.nodes["task-9"].dependencies == {}  # noqa

    def test_add_celery_group(self, some_sigs):
        wf = Workflow()
        sigs = some_sigs

        group = celery_canvas.group(sigs[:4])
        dependencies = [
            wf.add_signature(sigs[-2]),
            wf.add_signature(sigs[-1]),
        ]
        res = wf.add_celery_group(group, dependencies)
        assert set(res) == set([
                wf.nodes['task-0'], wf.nodes['task-1'],
                wf.nodes['task-2'], wf.nodes['task-3'],
            ])
        # use build_dependency_asserts(wf) to regenerate
        # build_dependency_asserts(wf)
        assert "task-8" in wf.nodes["task-0"].dependencies  # noqa
        assert "task-9" in wf.nodes["task-0"].dependencies  # noqa
        assert "task-8" in wf.nodes["task-1"].dependencies  # noqa
        assert "task-9" in wf.nodes["task-1"].dependencies  # noqa
        assert "task-8" in wf.nodes["task-2"].dependencies  # noqa
        assert "task-9" in wf.nodes["task-2"].dependencies  # noqa
        assert "task-8" in wf.nodes["task-3"].dependencies  # noqa
        assert "task-9" in wf.nodes["task-3"].dependencies  # noqa
        assert wf.nodes["task-8"].dependencies == {}  # noqa
        assert wf.nodes["task-9"].dependencies == {}  # noqa

    def test_add_celery_chord(self, some_sigs):
        wf = Workflow()
        sigs = some_sigs

        chord = celery_canvas.chord(sigs[:4], sigs[4])
        dependencies = [
            wf.add_signature(sigs[-2]),
            wf.add_signature(sigs[-1]),
        ]
        res = wf.add_celery_chord(chord, dependencies)

        assert res == [wf.nodes[sigs[4].id]]

        # use build_dependency_asserts(wf) to regenerate
        # build_dependency_asserts(wf)

        assert "task-8" in wf.nodes["task-0"].dependencies  # noqa
        assert "task-9" in wf.nodes["task-0"].dependencies  # noqa
        assert "task-8" in wf.nodes["task-1"].dependencies  # noqa
        assert "task-9" in wf.nodes["task-1"].dependencies  # noqa
        assert "task-8" in wf.nodes["task-2"].dependencies  # noqa
        assert "task-9" in wf.nodes["task-2"].dependencies  # noqa
        assert "task-8" in wf.nodes["task-3"].dependencies  # noqa
        assert "task-9" in wf.nodes["task-3"].dependencies  # noqa
        assert "task-0" in wf.nodes["task-4"].dependencies  # noqa
        assert "task-1" in wf.nodes["task-4"].dependencies  # noqa
        assert "task-2" in wf.nodes["task-4"].dependencies  # noqa
        assert "task-3" in wf.nodes["task-4"].dependencies  # noqa
        assert wf.nodes["task-8"].dependencies == {}  # noqa
        assert wf.nodes["task-9"].dependencies == {}  # noqa

    def test_add_chain_of_chords(self, some_sigs):
        wf = Workflow()
        sigs = some_sigs
        c1 = celery_canvas.chord(sigs[:4], sigs[4])
        c2 = celery_canvas.chord(sigs[5:9], sigs[9])
        canvas = celery_canvas.chain([c1, c2])

        res = wf.add_celery_canvas(canvas)

        assert res == [wf.nodes[sigs[9].id]]

        # use build_dependency_asserts(wf) to regenerate
        # build_dependency_asserts(wf)

        assert wf.nodes["task-0"].dependencies == {}  # noqa
        assert wf.nodes["task-1"].dependencies == {}  # noqa
        assert wf.nodes["task-2"].dependencies == {}  # noqa
        assert wf.nodes["task-3"].dependencies == {}  # noqa
        assert "task-0" in wf.nodes["task-4"].dependencies  # noqa
        assert "task-1" in wf.nodes["task-4"].dependencies  # noqa
        assert "task-2" in wf.nodes["task-4"].dependencies  # noqa
        assert "task-3" in wf.nodes["task-4"].dependencies  # noqa
        assert "task-4" in wf.nodes["task-5"].dependencies  # noqa
        assert "task-4" in wf.nodes["task-6"].dependencies  # noqa
        assert "task-4" in wf.nodes["task-7"].dependencies  # noqa
        assert "task-4" in wf.nodes["task-8"].dependencies  # noqa
        assert "task-5" in wf.nodes["task-9"].dependencies  # noqa
        assert "task-6" in wf.nodes["task-9"].dependencies  # noqa
        assert "task-7" in wf.nodes["task-9"].dependencies  # noqa
        assert "task-8" in wf.nodes["task-9"].dependencies  # noqa

    def test_add_empty_chord(self, some_sigs):
        """
            task -> chord([], task) -> task
            Verify if dependencies are built correctly
        """
        wf = Workflow()

        c0 = some_sigs[0]
        empty_chord = celery_canvas.chord([], some_sigs[1])
        c1 = some_sigs[2]

        canvas = c0 | empty_chord | c1

        wf.add_celery_canvas(canvas)

        # use build_dependency_asserts(wf) to regenerate
        # build_dependency_asserts(wf)
        assert wf.nodes["task-0"].dependencies == {}  # noqa
        assert "task-0" in wf.nodes["task-1"].dependencies  # noqa
        assert "task-1" in wf.nodes["task-2"].dependencies  # noqa

    def test_add_empty_group(self, some_sigs):
        """
            task -> group([])
            Verify if dependencies are built correctly
        """
        wf = Workflow()

        c0, c1 = some_sigs[0:2]
        empty_group = celery_canvas.group([])
        empty_group2 = celery_canvas.group([])

        # celery optimizes group | task into chord
        # so lets use group | group to verify if dependencies
        # are propagated properly
        canvas = c0 | empty_group | empty_group2 | c1

        wf.add_celery_canvas(canvas)

        # use build_dependency_asserts(wf) to regenerate
        # build_dependency_asserts(wf)
        assert wf.nodes["task-0"].dependencies == {}  # noqa
        assert "task-0" in wf.nodes["task-1"].dependencies  # noqa

    @pytest.mark.xfail(
        celery.__version__.startswith('4'),
        reason='Not supported on celery4'
    )
    @pytest.mark.xfail(
        celery.__version__.startswith('3'),
        reason='Chaining empty chain fails on celery3'
    )
    def test_add_empty_chain(self, some_sigs):
        """
            task -> chain([]) -> task
            Verify if dependencies are built correctly
        """
        wf = Workflow()

        c0 = some_sigs[0]
        empty_chain = celery_canvas.chain([])
        c1 = some_sigs[1]

        canvas = c0 | empty_chain | c1

        wf.add_celery_canvas(canvas)

        # use build_dependency_asserts(wf) to regenerate
        build_dependency_asserts(wf)
        assert wf.nodes["task-0"].dependencies == {}  # noqa
        assert "task-0" in wf.nodes["task-1"].dependencies  # noqa

    def test_add_complex(self, some_sigs):
        wf = Workflow()

        c0 = some_sigs[0]
        c1 = some_sigs[5]
        chain1 = celery_canvas.chain(some_sigs[1:3])
        chain2 = celery_canvas.chain(some_sigs[3:5])
        chord = celery_canvas.chord([chain1, chain2], c1)
        canvas = c0 | chord

        wf.add_celery_canvas(canvas)

        # use build_dependency_asserts(wf) to regenerate
        # build_dependency_asserts(wf)
        assert wf.nodes["task-0"].dependencies == {}  # noqa
        assert "task-0" in wf.nodes["task-1"].dependencies  # noqa
        assert "task-1" in wf.nodes["task-2"].dependencies  # noqa
        assert "task-0" in wf.nodes["task-3"].dependencies  # noqa
        assert "task-3" in wf.nodes["task-4"].dependencies  # noqa
        assert "task-2" in wf.nodes["task-5"].dependencies  # noqa
        assert "task-4" in wf.nodes["task-5"].dependencies  # noqa

    def test_simulate_run_chain_chord_chain(self, some_sigs):
        wf = Workflow()
        c0 = some_sigs[0]
        c1 = some_sigs[5]
        chain1 = celery_canvas.chain(some_sigs[1:3])
        chain2 = celery_canvas.chain(some_sigs[3:5])
        chord = celery_canvas.chord([chain1, chain2], c1)
        canvas = c0 | chord

        wf.add_celery_canvas(canvas)

        # use build_exec_asserts helper to rebuild asserts
        # build_exec_asserts(wf)
        assert wf.simulate_tick()
        assert wf.running == {'task-0': True}  # noqa
        assert wf.simulate_tick()
        assert wf.running == {'task-3': True, 'task-1': True}  # noqa
        assert wf.simulate_tick()
        assert wf.running == {'task-4': True, 'task-2': True}  # noqa
        assert wf.simulate_tick()
        assert wf.running == {'task-5': True}  # noqa
        assert not wf.simulate_tick()

    @pytest.mark.skipif(
        celery.__version__.startswith('3'),
        reason='Not supported on celery3'
    )
    def test_simulate_run_chain_group_chain(self, some_sigs):
        wf = Workflow()
        chain1 = celery_canvas.chain(some_sigs[:3])
        group1 = celery_canvas.group(some_sigs[3:7])
        chain2 = celery_canvas.chain(some_sigs[7:9])

        canvas = chain1 | group1 | chain2
        wf.add_celery_canvas(canvas)

        # use build_exec_asserts helper to rebuild asserts
        # build_exec_asserts(wf)
        assert wf.simulate_tick()
        assert wf.running == {'task-0': True}  # noqa
        assert wf.simulate_tick()
        assert wf.running == {'task-1': True}  # noqa
        assert wf.simulate_tick()
        assert wf.running == {'task-2': True}  # noqa
        assert wf.simulate_tick()
        assert wf.running == {'task-6': True, 'task-4': True, 'task-5': True, 'task-3': True}  # noqa
        assert wf.simulate_tick()
        assert wf.running == {'task-7': True}  # noqa
        assert wf.simulate_tick()
        assert wf.running == {'task-8': True}  # noqa
        assert not wf.simulate_tick()

    @pytest.mark.skipif(
        LooseVersion(celery.__version__) < LooseVersion('4'),
        reason='Not supported on celery3'
    )
    @pytest.mark.skipif(
        LooseVersion(celery.__version__) >= LooseVersion('4.4'),
        # https://github.com/celery/celery/pull/5613
        reason='Different behavior on celery 4.4+'
    )
    def test_simulate_run_group_group(self, some_sigs):
        wf = Workflow()
        group1 = celery_canvas.group(some_sigs[:4])
        group2 = celery_canvas.group(some_sigs[4:9])

        # What celery does here is actually # group() | group() -> single group
        # this is rather unexpected as group 2 should wait for all tasks from
        # group 1...
        canvas = group1 | group2
        wf.add_celery_canvas(canvas)

        # use build_exec_asserts helper to rebuild asserts
        # build_exec_asserts(wf)
        assert wf.simulate_tick()
        assert wf.running == {'task-8': True, 'task-6': True, 'task-7': True, 'task-4': True, 'task-5': True, 'task-2': True, 'task-3': True, 'task-0': True, 'task-1': True}  # noqa
        assert not wf.simulate_tick()

    @pytest.mark.skipif(
        LooseVersion(celery.__version__) < LooseVersion('4.4'),
        # https://github.com/celery/celery/pull/5613
        reason='Different behavior on celery 4.4+'
    )
    def test_simulate_run_group_group_fixed(self, some_sigs):
        wf = Workflow()
        group1 = celery_canvas.group(some_sigs[:4])
        group2 = celery_canvas.group(some_sigs[4:9])

        canvas = group1 | group2
        wf.add_celery_canvas(canvas)

        # use build_exec_asserts helper to rebuild asserts
        # build_exec_asserts(wf)
        assert wf.simulate_tick()
        assert wf.running == {'task-0': True, 'task-1': True, 'task-2': True, 'task-3': True}  # noqa
        assert wf.simulate_tick()
        assert wf.running == {'task-4': True, 'task-5': True, 'task-6': True, 'task-7': True, 'task-8': True}  # noqa
        assert not wf.simulate_tick()

    def test_to_from_dict(self, some_sigs):
        wf = Workflow()
        chain1 = celery_canvas.chain(some_sigs[:3])
        wf.add_celery_canvas(chain1)

        wf_dict = wf.to_dict()
        wf2 = Workflow.from_dict(wf_dict)

        assert list(wf.nodes.keys()) == list(wf2.nodes.keys())

        # use build_exec_asserts helper to rebuild asserts
        # build_exec_asserts(wf)
        assert wf.simulate_tick()
        assert wf.running == {'task-0': True}  # noqa
        assert wf.simulate_tick()
        assert wf.running == {'task-1': True}  # noqa
        assert wf.simulate_tick()
        assert wf.running == {'task-2': True}  # noqa
        assert not wf.simulate_tick()

    def test_freeze(self, app):
        wf = Workflow()
        sig = wf.freeze()

        assert isinstance(sig, entities.Signature)
        assert sig.id
        assert sig.kwargs
        assert wf.processing_limit_ts

    def test_get_retry_countdown(self):
        wf = Workflow()
        countdown = wf.get_retry_countdown()
        assert isinstance(countdown, int)

        # assumes default retry policy
        assert 10 <= countdown <= 30
