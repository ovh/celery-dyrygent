import hashlib
import os
import re
import sys
import uuid

from collections import defaultdict

from app import simple_task, read_logs

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'setup', 'worker', 'src', 'app'))


def md5(value):
    """Md5 check. The same function exists in worker."""
    return hashlib.md5(value.encode()).hexdigest()


def parse_logs(logs):
    grouped_logs = defaultdict(list)

    for log in logs.splitlines():
        findings = re.search(r'app.simple_task\[([\w-]+)\]: Test name: ([\w-]+) Test value: ([\w-]+)', log)
        if findings:
            task_id, test_name, test_value = findings.groups()
            grouped_logs[test_name].append(test_value)

    return grouped_logs


def test_simple():
    test_name = str(uuid.uuid4())
    result = simple_task.delay(test_name, '1')
    assert md5(f'{test_name} {result}') == result.get()

    logs_result = read_logs.delay()
    order = parse_logs(logs_result.get())[test_name]
    assert order == ['1']
