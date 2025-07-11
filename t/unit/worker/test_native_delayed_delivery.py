import itertools
from logging import LogRecord
from typing import Iterator
from unittest.mock import MagicMock, Mock, patch

import pytest
from kombu import Exchange, Queue
from kombu.utils.functional import retry_over_time

from celery.worker.consumer.delayed_delivery import MAX_RETRIES, RETRY_INTERVAL, DelayedDelivery


class test_DelayedDelivery:
    @patch('celery.worker.consumer.delayed_delivery.detect_quorum_queues', return_value=[False, ""])
    def test_include_if_no_quorum_queues_detected(self, _):
        consumer_mock = Mock()

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is False

    @patch('celery.worker.consumer.delayed_delivery.detect_quorum_queues', return_value=[True, ""])
    def test_include_if_quorum_queues_detected(self, _):
        consumer_mock = Mock()

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is True

    def test_start_native_delayed_delivery_direct_exchange(self, caplog):
        consumer_mock = MagicMock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='direct'))
        }

        delayed_delivery = DelayedDelivery(consumer_mock)

        delayed_delivery.start(consumer_mock)

        assert len(caplog.records) == 1
        record: LogRecord = caplog.records[0]
        assert record.levelname == "WARNING"
        assert record.message == (
            "Exchange celery is a direct exchange "
            "and native delayed delivery do not support direct exchanges.\n"
            "ETA tasks published to this exchange "
            "will block the worker until the ETA arrives."
        )

    def test_start_native_delayed_delivery_topic_exchange(self, caplog):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='topic'))
        }
        connection = MagicMock()
        consumer_mock.app.connection_for_write.return_value = connection

        delayed_delivery = DelayedDelivery(consumer_mock)

        delayed_delivery.start(consumer_mock)

        assert len(caplog.records) == 0
        # Verify connection context was called
        assert connection.__enter__.called
        assert connection.__exit__.called

    def test_start_native_delayed_delivery_fanout_exchange(self, caplog):
        consumer_mock = MagicMock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='fanout'))
        }

        delayed_delivery = DelayedDelivery(consumer_mock)

        delayed_delivery.start(consumer_mock)

        assert len(caplog.records) == 0

    @pytest.mark.parametrize(
        "broker_urls, expected_result",
        [
            ("amqp://", {"amqp://"}),
            ("amqp://;redis://", {"amqp://", "redis://"}),
            (
                ["amqp://", "redis://", "sqs://"],
                {"amqp://", "redis://", "sqs://"},
            ),
        ],
    )
    def test_validate_broker_urls_valid(self, broker_urls, expected_result):
        delayed_delivery = DelayedDelivery(Mock())
        urls = delayed_delivery._validate_broker_urls(broker_urls)
        assert urls == expected_result

    @pytest.mark.parametrize(
        "broker_urls, exception_type, exception_match",
        [
            ("", ValueError, "broker_url configuration is empty"),
            (None, ValueError, "broker_url configuration is empty"),
            ([], ValueError, "broker_url configuration is empty"),
            (123, ValueError, "broker_url must be a string or list"),
            (["amqp://", 123, None, "amqp://"], ValueError, "All broker URLs must be strings"),
        ],
    )
    def test_validate_broker_urls_invalid(self, broker_urls, exception_type, exception_match):
        delayed_delivery = DelayedDelivery(Mock())
        with pytest.raises(exception_type, match=exception_match):
            delayed_delivery._validate_broker_urls(broker_urls)

    def test_validate_queue_type_empty(self):
        delayed_delivery = DelayedDelivery(Mock())

        with pytest.raises(ValueError, match="broker_native_delayed_delivery_queue_type is not configured"):
            delayed_delivery._validate_queue_type(None)

        with pytest.raises(ValueError, match="broker_native_delayed_delivery_queue_type is not configured"):
            delayed_delivery._validate_queue_type("")

    def test_validate_queue_type_invalid(self):
        delayed_delivery = DelayedDelivery(Mock())

        with pytest.raises(ValueError, match="Invalid queue type 'invalid'. Must be one of: classic, quorum"):
            delayed_delivery._validate_queue_type("invalid")

    def test_validate_queue_type_valid(self):
        delayed_delivery = DelayedDelivery(Mock())

        delayed_delivery._validate_queue_type("classic")
        delayed_delivery._validate_queue_type("quorum")

    @patch('celery.worker.consumer.delayed_delivery.retry_over_time')
    def test_start_retry_on_connection_error(self, mock_retry, caplog):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://localhost;amqp://backup'
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='topic'))
        }

        mock_retry.side_effect = ConnectionRefusedError("Connection refused")

        delayed_delivery = DelayedDelivery(consumer_mock)
        delayed_delivery.start(consumer_mock)

        # Should try both URLs
        assert mock_retry.call_count == 2
        # Should log warning for each failed attempt
        assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 2
        # Should log critical when all URLs fail
        assert len([r for r in caplog.records if r.levelname == "CRITICAL"]) == 1

    def test_on_retry_logging(self, caplog):
        delayed_delivery = DelayedDelivery(Mock())
        exc = ConnectionRefusedError("Connection refused")

        # Create a dummy float iterator
        interval_range = iter([1.0, 2.0, 3.0])
        intervals_count = 1

        delayed_delivery._on_retry(exc, interval_range, intervals_count)

        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelname == "WARNING"
        assert "attempt 2/3" in record.message
        assert "Connection refused" in record.message

    def test_on_retry_argument_types(self):
        delayed_delivery_instance = DelayedDelivery(parent=Mock())
        fake_exception = ConnectionRefusedError("Simulated failure")

        # Define a custom errback to check types
        def type_checking_errback(self, exc, interval_range, intervals_count):
            assert isinstance(exc, Exception), f"Expected Exception, got {type(exc)}"
            assert isinstance(interval_range, Iterator), f"Expected Iterator, got {type(interval_range)}"
            assert isinstance(intervals_count, int), f"Expected int, got {type(intervals_count)}"

            peek_iter, interval_range = itertools.tee(interval_range)
            try:
                first = next(peek_iter)
                assert isinstance(first, float)
            except StopIteration:
                pass

            return 0.1

        # Patch _setup_delayed_delivery to raise the exception immediately
        with patch.object(delayed_delivery_instance, '_setup_delayed_delivery', side_effect=fake_exception):
            # Patch _on_retry properly as a bound method to avoid 'missing self'
            with patch.object(
                delayed_delivery_instance,
                '_on_retry',
                new=type_checking_errback.__get__(delayed_delivery_instance)
            ):
                try:
                    with pytest.raises(ConnectionRefusedError):
                        retry_over_time(
                            delayed_delivery_instance._setup_delayed_delivery,
                            args=(Mock(), "amqp://localhost"),
                            catch=(ConnectionRefusedError,),
                            errback=delayed_delivery_instance._on_retry,
                            interval_start=RETRY_INTERVAL,
                            max_retries=MAX_RETRIES,
                        )
                except ConnectionRefusedError:
                    pass  # expected

    def test_retry_over_time_with_float_return(self):
        delayed_delivery = DelayedDelivery(parent=Mock())
        return_values = []

        # Wrap the real _on_retry method to capture its return value
        original_on_retry = delayed_delivery._on_retry

        def wrapped_on_retry(exc, interval_range, intervals_count):
            result = original_on_retry(exc, interval_range, intervals_count)
            return_values.append(result)
            return result

        with patch.object(
            delayed_delivery, '_setup_delayed_delivery',
            side_effect=ConnectionRefusedError("Simulated failure")
        ):
            with pytest.raises(ConnectionRefusedError):
                retry_over_time(
                    fun=delayed_delivery._setup_delayed_delivery,
                    args=(Mock(), "amqp://localhost"),
                    catch=(ConnectionRefusedError,),
                    errback=wrapped_on_retry,
                    interval_start=RETRY_INTERVAL,
                    max_retries=MAX_RETRIES
                )

        assert len(return_values) == MAX_RETRIES
        for value in return_values:
            assert isinstance(value, float), f"Expected float, got {type(value)}"

    def test_start_with_no_queues(self, caplog):
        consumer_mock = MagicMock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.app.amqp.queues = {}

        delayed_delivery = DelayedDelivery(consumer_mock)
        delayed_delivery.start(consumer_mock)

        assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 1
        assert "No queues found to bind for delayed delivery" in caplog.records[0].message

    def test_start_configuration_validation_error(self, caplog):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_url = ""  # Invalid broker URL

        delayed_delivery = DelayedDelivery(consumer_mock)

        with pytest.raises(ValueError, match="broker_url configuration is empty"):
            delayed_delivery.start(consumer_mock)

        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelname == "CRITICAL"
        assert "Configuration validation failed" in record.message

    @patch('celery.worker.consumer.delayed_delivery.declare_native_delayed_delivery_exchanges_and_queues')
    def test_setup_declare_error(self, mock_declare, caplog):
        consumer_mock = MagicMock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='topic'))
        }

        mock_declare.side_effect = Exception("Failed to declare")

        delayed_delivery = DelayedDelivery(consumer_mock)
        delayed_delivery.start(consumer_mock)

        # Should log warning and critical messages
        assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 2
        assert len([r for r in caplog.records if r.levelname == "CRITICAL"]) == 1
        assert any("Failed to declare exchanges and queues" in r.message for r in caplog.records)
        assert any("Failed to setup delayed delivery for all broker URLs" in r.message for r in caplog.records)

    @patch('celery.worker.consumer.delayed_delivery.bind_queue_to_native_delayed_delivery_exchange')
    def test_setup_bind_error(self, mock_bind, caplog):
        consumer_mock = MagicMock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='topic'))
        }

        mock_bind.side_effect = Exception("Failed to bind")

        delayed_delivery = DelayedDelivery(consumer_mock)
        delayed_delivery.start(consumer_mock)

        # Should log warning and critical messages
        assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 2
        assert len([r for r in caplog.records if r.levelname == "CRITICAL"]) == 1
        assert any("Failed to bind queue" in r.message for r in caplog.records)
        assert any("Failed to setup delayed delivery for all broker URLs" in r.message for r in caplog.records)
