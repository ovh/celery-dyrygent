# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The celery-dyrygent Authors. All rights reserved.

from celery_dyrygent.workflows.exceptions import WorkflowException
from celery_dyrygent.celery import entities


class WorkflowNode(object):
    """
    Class wrapping single signature, adding workflow related stuff
    """
    def __init__(self, signature):
        assert isinstance(signature, entities.Signature)
        self.signature = signature

        if signature.id is None:
            # need to have consistent signature IDs to build relations
            raise WorkflowException(
                "Signature '{}' has no ID, it is not frozen"
                .format(signature)
            )

        self.id = signature.id
        assert self.id is not None

        # store node dependencies either it's None or dict
        # dict will be able to hold additional info, e.g to run task only if
        # on of dependency fails e.g. at least twice
        self.dependencies = {}
        self.custom_payload = {}

    def add_dependency(self, other_node, options=None):
        assert isinstance(other_node, WorkflowNode)
        if options is not None:
            assert isinstance(options, dict)

        if other_node.id in self.dependencies:
            raise WorkflowException(
                "Dependency '{}' already added"
                .format(other_node.signature)
            )

        self.dependencies[other_node.id] = options

    def to_dict(self):
        """
        Packs internals into a serializable dict
        """
        return dict(
            id=self.id,
            # celery signatures are serializable so it will work
            signature=self.signature,
            dependencies=self.dependencies,
            custom_payload=self.custom_payload,
        )

    @classmethod
    def from_dict(cls, data_dict):
        """
        Unpacks dict into WorkflowNode
        """
        # possible optiomization here - lazy initialize signatures
        sig = entities.Signature(data_dict['signature'])
        obj = cls(sig)
        obj.dependencies = data_dict['dependencies']
        obj.custom_payload = data_dict.get('custom_payload', {})
        return obj
