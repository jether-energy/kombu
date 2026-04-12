from __future__ import absolute_import

from concurrent.futures import Future

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
    return ch


class TestPutPublishReliability(Case):
    """
    Channel._put and _put_fanout must propagate publish failures, not silently swallow them.
    """

    def test_put_raises_on_publish_failure(self):
        error = Exception('Transient PubSub publish failure')
        ch = _make_mock_channel(_make_failing_future(error))

        with self.assertRaises(Exception) as ctx:
            Channel._put(ch, 'test-queue', TEST_MESSAGE)

        self.assertIn('Transient PubSub publish failure', str(ctx.exception))

    def test_put_fanout_raises_on_publish_failure(self):
        error = Exception('Transient PubSub publish failure')
        ch = _make_mock_channel(_make_failing_future(error))

        with self.assertRaises(Exception) as ctx:
            Channel._put_fanout(
                ch,
                'test-exchange',
                TEST_MESSAGE,
                routing_key='test-key',
            )

        self.assertIn('Transient PubSub publish failure', str(ctx.exception))

    def test_put_succeeds_when_publish_succeeds(self):
        ch = _make_mock_channel(_make_success_future())

        Channel._put(ch, 'test-queue', TEST_MESSAGE)

        ch.publisher.publish.assert_called_once()

    def test_put_fanout_succeeds_when_publish_succeeds(self):
        ch = _make_mock_channel(_make_success_future())

        Channel._put_fanout(
            ch,
            'test-exchange',
            TEST_MESSAGE,
            routing_key='test-key',
        )

        ch.publisher.publish.assert_called_once()
