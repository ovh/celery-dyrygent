# Celery Dyrygent

[![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)](https://www.python.org/downloads/release/python-360/)
![Unit Tests](https://github.com/ovh/celery-dyrygent/actions/workflows/unit-tests.yml/badge.svg)
![Integration Tests](https://github.com/ovh/celery-dyrygent/actions/workflows/integration-tests.yml/badge.svg)


This project aims to support full DAG workflow processing.
It's designed as celery extension and uses celery as an execution backend.
Celery-dyrygent is released under modified BSD license. See [license](LICENSE)

## What is it?
The reasons behind this project so as the implementation details were described in the following blogpost
https://www.ovh.com/blog/doing-big-automation-with-celery/

### What is a DAG workflow?
DAG is a shortcut for Directed Acyclic Graph.
While DAG workflow would be any combination of celery primitives:
- groups
- chains
- chords

Celery Dyrygent is able to process any kind of DAG workflows.

### Why not to use native celery stuff?
Celery struggles a bit with complex workflows built from combining primitives.
The execution might be unreliable, there are a lot of corner cases where workflow might not work as desired.
Serialization of complex workflows causes memory issues.
Some of the encountered problems which aren't solved (celery 4.2.1):
- https://github.com/celery/celery/issues/5000
- https://github.com/celery/celery/issues/5286
- https://github.com/celery/celery/issues/5327

## How does it work?
The whole workflow machinery works simialar to ```chord_unlock``` repeating celery task which waits till some tasks are done (header) and then executes further tasks (body).
Celery Dyrygent introduces a workflow processor task which orchestrates an execution of a whole workflow.
Once the workflow is started the workflow processor task is repeated till the workflow execution is done or till some TTL timestamp is reached (not to repeat indefinitely).
The workflow processor schedules the execution of tasks according to their relations, retries itself, then checks if the tasks are done so the new ones can be scheduled, repeat.
That's it, the idea is quite simple.

### Advantages
- execution part is done by Celery, so all celery machinery with its features is available (retries, countdowns, etc.)
- each workflow is executed in the same way
- Celery operates on simple tasks only - no nested structures which causes troubles
- link error for whole workflow can be implemented
- finalizing task for whole workflow can be implemented (e.g. do something always when workflow finishes)
- workflow execution is SOLID and RELIABLE
- it's possible to track progress through signals (might need to implement a new signal for each tick)

### Drawbacks
- At the moment workflow processor doesn't pass task results from precedign tasks to following tasks (can be implemented, not implemented at the moment).
- Workflow processor task is doing repeating ticks (like celery chord unlock) and new tasks are scheduled only within the ticks. This may result in noticeably longer execution time of task chains (e.g. if ticks are done each 2s, next task in chain will only be each 2s)
- Reliable result backend has to be enabled

# How to use it?
## Which celery versions are supported?
- celery 3.1.25
- celery 4.2.1
- probably any celery 4.x

## Integration
### Initialize workflows
You need to register workflow processor task in your celery app
```python
from celery_dyrygent.tasks import register_workflow_processor

app = Celery() #  your celery application instance

workflow_processor = register_workflow_processor(app)
```

### Use workflow on you celery canvas
Workflows can consume celery canvas to properly build internal relations
```python
from celery_dyrygent.workflows import Workflow

canvas = celery.chain() | celery.chord() #  define your canvas using native celery mechanisms

wf = Workflow()
wf.add_celery_canvas(canvas)
wf.apply_async()
```

Workflow processor task will be scheduled holding all signatures from canvas and their relations. It will execute signatures according to their relations.

### Signals support
Celery Dyrygent provides additional signals which can be used e.g. for tracking workflow progress. Following signals are available:
- ```after_active_tick```
- ```on_finish```
- ```on_state_change```

#### How to use signals?
When a signal is emitted all registered signal handlers are executed. In order to register signal handler you need to use ```Workflow.connect``` function. See examples below. The handler is called with two parameters: workflow instance and payload (optional).

#### Using ```on_state_change``` signal
Signal is emitted when workflow state changes. Supported states are:
- INITIAL
- RUNNING
- SUCCESS
- FAILURE
- ERROR

Handler is called with two params:
- workflow instance
- payload - current state of a workflow

``` python
from celery_dyrygent.workflows import Workflow

@Workflow.connect('on_state_change')
def handle_state_change(workflow, payload):
    print(
        "Workflow {} has new state {}"
        .format(workflow.id, payload)
    )
```

#### Using ```on_finish``` signal
Signal is emitted when workflow is finished (or can't move forward due to failed tasks)

Handler params are:
- workflow instance
- paylod - None

#### Using ```after_active_tick```
Signal is emitted when workflow has scheduled new tasks

Handler params are:
- workflow instance
- payload - None


### Support for custom data
Both `Workflow`and `WorkflowNode` have a `custom_payload` dictionary member that can be used to store 
additional data. For example, one can use those dictionnary to store some application specific 
metadata.

```python
...
wf = Workflow()
for task in task_list:
    sig = create_celery_task(task)
    sig.freeze()
    node = wf.add_signature(sig)
    node.custom_payload['user_id'] = task.user_id
...
```

#### Using celery task options
You can define custom options for your tasks, as defined in:
https://docs.celeryproject.org/en/stable/reference/celery.app.task.html#celery.app.task.Task.apply_async

These options may be different between the workflow task and user tasks.
``` python
wf = Workflow(options={'priority': 10})
wf.add_celery_canvas(canvas)
wf.apply_async(options={'priority': 8})
```

## TODO
- Proper documentation (e.g. sphinx)
- Some integration tests?
- Python pip release
