# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

"""
This module should contain all dependencies linked to Celery modules.
Since it suppose to support celery 3 and celery 4
"""

from . import inspect, entities

__all__ = [
    'entities',
    'inspect'
]
