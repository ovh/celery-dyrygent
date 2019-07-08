# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

from celery import canvas as celery_canvas

from celery_dyrygent.celery import (
    inspect,
)


class TestCeleryInspect(object):
    def test_chain_tasks(self):
        sigs = [celery_canvas.Signature() for r in range(4)]

        chain = celery_canvas.chain(sigs)
        tasks = inspect.get_chain_tasks(chain)
        assert sigs == tasks

    def test_chord_tasks(self):
        sigs = [celery_canvas.Signature() for r in range(4)]
        barrier = celery_canvas.Signature()

        chord = celery_canvas.chord(sigs, barrier)

        body, tasks = inspect.get_chord_tasks(chord)
        assert tasks == sigs
        assert body == barrier

    def test_group_tasks(self):
        sigs = [celery_canvas.Signature() for r in range(4)]
        group = celery_canvas.group(sigs)
        tasks = inspect.get_group_tasks(group)
        assert tasks == sigs

    def test_signature_freeze(self):
        # perhaps this isn't the best place for this test,
        # it verifies whether freeze() still works as expected - assignes id
        sig = celery_canvas.Signature()
        sig.freeze()
        assert sig.id
