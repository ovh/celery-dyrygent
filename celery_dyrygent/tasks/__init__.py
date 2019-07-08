# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

from celery_dyrygent.tasks.processor import (
    workflow_processor, register_workflow_processor
)


__all__ = [
    'register_workflow_processor',
    'workflow_processor',
]
