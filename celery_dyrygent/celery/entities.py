# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

import celery
from billiard import exceptions as billiard_exceptions
from celery import canvas as celery_canvas
from celery.result import (
    AsyncResult,
)

Signature = celery_canvas.Signature
Chord = celery_canvas.chord

try:
    celery.version_info
except AttributeError:
    # seems we're dealing with celery 3
    Chain = celery_canvas.chain
else:
    Chain = celery_canvas._chain

Group = celery_canvas.group

AsyncResult = AsyncResult

Celery = celery.Celery

WorkerLostError = billiard_exceptions.WorkerLostError
