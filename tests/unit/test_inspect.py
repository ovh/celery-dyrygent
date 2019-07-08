# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

import pytest
import mock
from celery_dyrygent.celery.entities import AsyncResult, WorkerLostError
from celery_dyrygent.celery.inspect import ResultState


class TestResultState(object):
    @pytest.mark.parametrize('res_state, res_val, ready, expected', [
        (u'PENDING', None, False, ResultState.INIT),
        (u'STARTED', None, False, ResultState.RUNNING),
        (u'FAILED', None, True, ResultState.DONE),
        (
            u'FAILED',
            Exception('Worker exited prematurely: exitcode 155.'),
            True, ResultState.DONE
        ),
        (
            u'FAILED',
            WorkerLostError('Worker exited prematurely: exitcode 155.'),
            True, ResultState.FAILED_RETRYING
        ),
        (u'SUCCESS', 0, True, ResultState.DONE),
        (
            u'FAILED',
            'Worker exited prematurely: exitcode 155.',
            True, ResultState.DONE
        ),
        (
            u'FAILED',
            TypeError("unhashable type: 'list'"),
            True, ResultState.RESCHEDULE
        ),
    ])
    def test_state(self, res_state, res_val, ready, expected):
        res = mock.Mock(spec=AsyncResult)
        res.state = res_state
        res.ready.return_value = ready
        res.result = res_val
        result = ResultState(res)

        assert result.state == expected

    def test_success(self):
        res = mock.Mock(spec=AsyncResult)
        result = ResultState(res)
        assert result.success == res.successful()

    @pytest.mark.parametrize('success', [
        True, False,
    ])
    def test_is_successful(self, success):
        res = mock.Mock(spec=AsyncResult)
        result = ResultState(res)
        result.success = success
        assert result.is_successful() == success

    @pytest.mark.parametrize('ready, expected', [
        (False, False),
        (True, True),
    ])
    def test_is_done(self, ready, expected):
        res = mock.Mock(spec=AsyncResult)
        res.ready.return_value = ready
        res.state = u'DUMMY'
        result = ResultState(res)
        assert result.is_done() == expected

    @pytest.mark.parametrize('state, expected', [
        (ResultState.RESCHEDULE, True),
        (ResultState.RUNNING, True),
        (ResultState.INIT, False),
        (ResultState.DONE, True),
        (ResultState.FAILED_RETRYING, True),
    ])
    def test_was_published(self, state, expected):
        result = ResultState(mock.Mock(spec=AsyncResult))
        result.state = state
        assert result.was_published() == expected

    @pytest.mark.parametrize('state, expected', [
        (ResultState.RESCHEDULE, True),
        (ResultState.RUNNING, False),
        (ResultState.INIT, False),
        (ResultState.DONE, False),
        (ResultState.FAILED_RETRYING, False),
    ])
    def test_needs_reschedule(self, state, expected):
        result = ResultState(mock.Mock(spec=AsyncResult))
        result.state = state
        assert result.needs_reschedule() == expected

    @pytest.mark.parametrize('state, expected', [
        (ResultState.RESCHEDULE, True),
        (ResultState.RUNNING, False),
        (ResultState.INIT, False),
        (ResultState.DONE, True),
        (ResultState.FAILED_RETRYING, False),
    ])
    def test_is_final(self, state, expected):
        result = ResultState(mock.Mock(spec=AsyncResult))
        result.state = state
        assert result.is_final() == expected
