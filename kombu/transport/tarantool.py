"""Tarantool transport module for Kombu.

Features
========
* Type: Virtual
* Supports Direct: Yes
* Supports Topic: Yes
* Supports Fanout: Yes
* Supports Priority: Yes
* Supports TTL: Yes

Connection String
=================
Connection string is in the following format:

.. code-block::

    tarantool://TARANTOOL_ADDRESS[:PORT]

"""
from queue import Empty
from . import virtual
from ..utils import cached_property


try:
    import tarantool
except ImportError:  # pragma: no cover
    tarantool = None  # noqa

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 3301

__author__ = 'Artyom Dubinin <artyom.dubinin@corp.mail.ru>'


class Channel(virtual.Channel):
    """Tarantool Channel."""

    supports_fanout = True
    # restore(release) doing on tarantool side
    do_restore = False
    auto_delete = set()

    def __init__(self, connection, **kwargs):
        super().__init__(connection, **kwargs)
        vhost = self.connection.client.virtual_host
        self._vhost = '/{}'.format(vhost.strip('/'))
        if not self.client.connected:
            raise

    def _new_queue(self, queue, **kwargs):
        if not self._has_queue(queue):
            self._create_queue(queue)

        self.client.eval(
            "queue.tube.{}.auto_delete = ...".format(queue),
            (kwargs['auto_delete']))

    def _has_queue(self, queue, **kwarg):
        return self.client.call("queue.tube", queue).data[0]

    def _is_auto_delete(self, queue):
        return self.client.eval(
            "return queue.tube.{}.auto_delete".format(queue),
        ).data[0]

    def _create_queue(self, queue_name):
        self.client.eval(
            "queue.create_tube(...)", (queue_name, 'fifottl'))

# AttributeError: 'Channel' object has no attribute '_queue_bind'
    def _queue_bind(self, *args):
        pass

    # it's not a virtual base func, but it was needed to support fanout
    def _put_fanout(self, exchange, message, routing_key=None, **kwargs):
        for queue in self._lookup(exchange, routing_key):
            self._put(queue, message)

    def _put(self, queue, message, **kwargs):
        ttl = 'nil'
        if 'expiration' in message['properties']:
            # kombu expiration in ms, but tarantool get seconds
            ttl = int(message['properties']['expiration']) / 1000

        cmd = 'return queue.tube.{0}:put(...,{{pri={1},ttl={2}}})'
        return self.client.eval(
            cmd.format(queue, self._get_message_priority(message), ttl),
            message)

    def _get(self, queue, timeout=None):
        res = self.client.call("queue.tube.{}:take".format(queue), (1))
        if not res.data:
            raise Empty
        task_id = res.data[0][0]
        message = res.data[0][2]
        if self._is_auto_delete(queue):
            self.auto_delete.add(message['properties']['delivery_tag'])
            self._ack(queue, task_id)
        else:
            message['properties']['delivery_info'].update({'queue': queue})
            message['properties']['delivery_info'].update({'task_id': task_id})
        return message

    def _ack(self, queue, id):
        self.client.call(f"queue.tube.{queue}:ack", id)

    def basic_ack(self, delivery_tag, multiple=False):
        if delivery_tag not in self.auto_delete:
            queue = self.qos.get(delivery_tag).delivery_info['queue']
            task_id = self.qos.get(delivery_tag).delivery_info['task_id']
            self._ack(queue, task_id)
            del self.qos._delivered[delivery_tag]

    def _size(self, queue):
        # TODO: only ready size returned, need count of all messages
        if self._has_queue(queue):
            stat = self.client.call('queue.statistics', queue)
            return stat.data[0]['tasks']['ready']
        return None

    def _drop(self, queue):
        self.client.call(f"queue.tube.{queue}:drop")

    def _purge(self, queue):
        if self._has_queue(queue):
            self.client.call("queue.tube.{}:truncate".format(queue))

    def _open(self):
        conninfo = self.connection.client
        host = conninfo.hostname or DEFAULT_HOST
        host_port = conninfo.port or DEFAULT_PORT
        conn = tarantool.connect(host, host_port)
        return conn

    def close(self):
        self.client.close()

    @cached_property
    def client(self):
        return self._open()


class Transport(virtual.Transport):
    """In-memory Transport."""

    Channel = Channel
    polling_interval = 1
    default_port = DEFAULT_PORT
    driver_type = 'tarantool'
    driver_name = 'tarantool'

    def __init__(self, *args, **kwargs):
        if tarantool is None:
            raise ImportError('The tarantool library is not installed')

        super().__init__(*args, **kwargs)

    def driver_version(self):
        return tarantool.__version__
