from __future__ import absolute_import

import logging
import threading
import time
from concurrent.futures import Future
from queue import Queue

from kombu.tests.case import Case, MagicMock
from kombu.transport.gcpubsub import Channel, QueueDescriptor


def _make_failing_future(error):
    f = Future()
    f.set_exception(error)
    return f


def _make_success_future(result='msg-id-123'):
    f = Future()
    f.set_result(result)
    return f


TEST_MESSAGE = {
    'body': 'test-payload',
    'properties': {'delivery_info': {'routing_key': 'test-key'}},
}


def _make_mock_channel(publish_return_value):
    ch = MagicMock()
    ch.entity_name.side_effect = lambda name: 'kombu-{0}'.format(name)
    ch._get_routing_key.return_value = 'test-key'
    ch._queue_cache = {
        'kombu-test-queue': QueueDescriptor(
            name='kombu-test-queue',
            topic_path='projects/test-project/topics/kombu-test-queue',
            subscription_id='kombu-test-queue',
            subscription_path='projects/test-project/subscriptions/kombu-test-queue',
        )
    }
    ch.publisher.publish.return_value = publish_return_value
    ch.publisher.topic_path.return_value = 'projects/test-project/topics/kombu-test-exchange'
    ch.retry_timeout_seconds = 10
    ch._fanout_exchanges = set()
    ch._pending_publish_futures = Queue()
    return ch


class TestPutPublishReliability(Case):
    """
    Channel._put and _put_fanout must not block on publish results.
    Failures are handled asynchronously by the publish waiter thread,
    which logs (but does not raise) on failure.
    """

    def test_put_does_not_raise_on_publish_failure(self):
        error = Exception('Transient PubSub publish failure')
        ch = _make_mock_channel(_make_failing_future(error))

        Channel._put(ch, 'test-queue', TEST_MESSAGE)

        # future is queued for the waiter thread, not awaited here
        self.assertEqual(ch._pending_publish_futures.qsize(), 1)

    def test_put_fanout_does_not_raise_on_publish_failure(self):
        error = Exception('Transient PubSub publish failure')
        ch = _make_mock_channel(_make_failing_future(error))

        Channel._put_fanout(
            ch,
            'test-exchange',
            TEST_MESSAGE,
            routing_key='test-key',
        )

        self.assertEqual(ch._pending_publish_futures.qsize(), 1)

    def test_put_queues_future(self):
        success_future = _make_success_future()
        ch = _make_mock_channel(success_future)

        Channel._put(ch, 'test-queue', TEST_MESSAGE)

        ch.publisher.publish.assert_called_once()
        self.assertIs(ch._pending_publish_futures.get_nowait(), success_future)

    def test_put_fanout_queues_future(self):
        success_future = _make_success_future()
        ch = _make_mock_channel(success_future)

        Channel._put_fanout(
            ch,
            'test-exchange',
            TEST_MESSAGE,
            routing_key='test-key',
        )

        ch.publisher.publish.assert_called_once()
        self.assertIs(ch._pending_publish_futures.get_nowait(), success_future)


class TestPublishWaiterThread(Case):
    """The publish waiter thread drains futures and logs failures."""

    def _run_waiter_once(self, ch):
        # Run the waiter loop body just long enough to drain the queue
        ch._stop_publish_waiter = threading.Event()

        def stop_when_empty():
            while ch._pending_publish_futures.qsize() > 0:
                time.sleep(0.01)
            ch._stop_publish_waiter.set()

        stopper = threading.Thread(target=stop_when_empty, daemon=True)
        stopper.start()
        Channel._wait_for_publish_futures(ch)
        stopper.join(timeout=2)

    def test_waiter_logs_publish_failures(self):
        ch = MagicMock()
        ch.retry_timeout_seconds = 1
        ch._pending_publish_futures = Queue()
        ch._pending_publish_futures.put(
            _make_failing_future(Exception('boom'))
        )

        with self.assertLogs('kombu.transport.gcpubsub', level=logging.ERROR) as cm:
            self._run_waiter_once(ch)

        self.assertTrue(
            any('publish failed' in m and 'boom' in m for m in cm.output),
            'expected publish failure log, got: {0}'.format(cm.output),
        )

    def test_waiter_does_not_log_on_success(self):
        ch = MagicMock()
        ch.retry_timeout_seconds = 1
        ch._pending_publish_futures = Queue()
        ch._pending_publish_futures.put(_make_success_future())

        logger = logging.getLogger('kombu.transport.gcpubsub')
        with self.assertLogs(logger, level=logging.ERROR) as cm:
            # assertLogs requires at least one record; emit a sentinel
            logger.error('sentinel')
            self._run_waiter_once(ch)

        publish_errors = [m for m in cm.output if 'publish failed' in m]
        self.assertEqual(publish_errors, [])
