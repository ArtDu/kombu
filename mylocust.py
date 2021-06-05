#
# run command: locust -f mylocust.py --host=localhost:8000
#
import datetime
import inspect
import time
import uuid

from kombu import Connection

from locust import User, TaskSet, task, events



def stopwatch(func):
    def wrapper(*args, **kwargs):
        # get task's function name
        previous_frame = inspect.currentframe().f_back
        _, _, task_name, _, _ = inspect.getframeinfo(previous_frame)

        start = time.time()
        result = None
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            total = int((time.time() - start) * 1000)
            events.request_failure.fire(request_type="TYPE",
                                        name=task_name,
                                        response_time=total,
                                        response_length=0,
                                        exception=e)
        else:
            total = int((time.time() - start) * 1000)
            events.request_success.fire(request_type="TYPE",
                                        name=task_name,
                                        response_time=total,
                                        response_length=0)
        return result

    return wrapper


class KombuClient:
    @stopwatch
    def test_simple_queue(self, simple_queue):
        text = f'helloworld, sent at ' \
               f'{datetime.datetime.today()}'
        simple_queue.put(text)
        message = simple_queue.get()
        assert message.decode() == text
        message.ack()


class ProtocolKombu(User):
    abstract = True

    def __init__(self, environment):
        super().__init__(environment)
        self.client = KombuClient()


class KombuTasks(TaskSet):
    c = None
    simple_queue = None

    def on_start(self):
        print('connect to broker')
        self.c = Connection(transport='tarantool')
        self.simple_queue = \
            self.c.SimpleQueue('simple_queue_' +
                               str(uuid.uuid4()).replace('-', ''))

    def on_stop(self):
        print('close broker connection')
        self.simple_queue.close()
        self.c.close()

    @task
    def check_simple_queue(self):
        self.client.test_simple_queue(self.simple_queue)


class Entrypoint(ProtocolKombu):
    tasks = [KombuTasks]


