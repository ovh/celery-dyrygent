import hashlib

from os import environ
from celery import Celery
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

broker_url = environ['BROKER_URL']
result_backend = environ['RESULT_BACKEND']

app = Celery('tasks', broker=broker_url, backend=result_backend)


def md5(value):
    """Md5 check. The same function exists in master."""
    return hashlib.md5(value.encode()).hexdigest()


@app.task(bind=True)
def simple_task(self, test_name, test_value):
    logger.info('Test name: %s Test value: %s', test_name, test_value)
    return md5(f'{test_name} {self.request.id}')


@app.task()
def read_logs():
    with open('celery.logs', 'r') as reader:
        logs = reader.read()
        return '\n'.join([log for log in logs.splitlines() if 'Test name' in log])
