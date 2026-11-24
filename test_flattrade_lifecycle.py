import asyncio
import json
import threading
import unittest
from unittest.mock import AsyncMock, Mock, patch

import main
from flattrade import flattrade_websocket as stream


class FlattradeLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_authentication_rejection_is_not_treated_as_transport_failure(self):
        api = Mock()
        api.set_session.return_value = True
        api.start_websocket.side_effect = lambda **callbacks: callbacks[
            "socket_error_callback"
        ]({"t": "ck", "s": "Not_Ok", "emsg": "Session expired"})
        with patch.object(stream, "api", api), \
             patch.object(stream, "loop", asyncio.get_running_loop()), \
             patch.object(stream, "broker_ready", asyncio.Event()), \
             patch.object(stream, "broker_disconnected", asyncio.Event()), \
             patch.object(stream, "broker_error_event", asyncio.Event()):
            with self.assertRaises(stream.BrokerAuthenticationRejected):
                await stream.setup_api_connection("fake-session", "fake-user")

    async def test_sdk_wrapper_performs_only_one_transport_attempt(self):
        api = Mock()
        websocket_app = Mock()
        stop_event = threading.Event()
        setattr(api, "_NorenApi__websocket", websocket_app)
        setattr(api, "_NorenApi__stop_event", stop_event)

        stream._configure_sdk_single_attempt(api)
        getattr(api, "_NorenApi__ws_run_forever")()

        websocket_app.run_forever.assert_called_once_with(
            ping_interval=3,
            ping_payload='{"t":"h"}',
        )

    async def test_only_numeric_quotes_advance_market_data_freshness(self):
        with patch.object(stream, "last_tick_at", None), \
             patch.object(stream, "loop", None), \
             patch.object(stream, "quote_queue", asyncio.Queue()):
            stream.event_handler_quote_update({"tk": "26000", "lp": "invalid"})
            self.assertIsNone(stream.last_tick_at)
            stream.event_handler_quote_update({"tk": "26000", "lp": "123.4"})
            self.assertIsNotNone(stream.last_tick_at)

    async def test_retry_delay_is_jittered_but_remains_bounded(self):
        with patch.object(stream.random, "uniform", return_value=1.25):
            self.assertEqual(stream._retry_delay(1), 1.25)
            self.assertEqual(stream._retry_delay(100), stream.MAX_RETRY_DELAY_SECONDS)

    async def test_sdk_shutdown_signals_and_joins_worker(self):
        api = Mock()
        stop_event = threading.Event()
        websocket_app = Mock()
        worker = Mock()
        worker.is_alive.side_effect = [True, False]
        setattr(api, "_NorenApi__stop_event", stop_event)
        setattr(api, "_NorenApi__websocket", websocket_app)
        setattr(api, "_NorenApi__ws_thread", worker)

        await stream._stop_sdk_websocket(api)

        self.assertTrue(stop_event.is_set())
        websocket_app.close.assert_called_once()
        worker.join.assert_called_once_with(5)

    async def test_broadcast_sends_each_quote_to_every_connected_client(self):
        queue = asyncio.Queue()
        first_client = Mock()
        first_client.send = AsyncMock()
        second_client = Mock()
        second_client.send = AsyncMock()
        task = None
        with patch.object(stream, "quote_queue", queue), \
             patch.object(stream, "connected_clients", {first_client, second_client}):
            task = asyncio.create_task(stream._broadcast_quote_updates())
            queue.put_nowait({"tk": "26000", "lp": "123.4"})
            await asyncio.wait_for(
                asyncio.gather(first_client.send.awaited.wait(), second_client.send.awaited.wait()),
                timeout=1,
            )
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        first_client.send.assert_awaited_once_with('{"tk": "26000", "lp": "123.4"}')
        second_client.send.assert_awaited_once_with('{"tk": "26000", "lp": "123.4"}')

    async def test_shared_symbol_is_unsubscribed_only_after_last_client_leaves(self):
        first_client = object()
        second_client = object()
        api = Mock()
        with patch.object(stream, "api", api), \
             patch.object(stream, "stream_health", {"authenticated": True}), \
             patch.object(
                 stream,
                 "client_subscriptions",
                 {first_client: {"NSE|26000"}, second_client: {"NSE|26000"}},
             ), \
             patch.object(stream, "subscribed_symbols", {"NSE|26000"}):
            await stream._release_client_subscriptions(first_client)
            api.unsubscribe.assert_not_called()
            await stream._release_client_subscriptions(second_client)

        api.unsubscribe.assert_called_once_with(instrument=["NSE|26000"], feed_type=2)

    async def test_invalid_session_response_does_not_crash_supervisor(self):
        response = Mock()
        response.json.return_value = []
        with patch.object(stream.requests, "get", return_value=response):
            result = await stream.get_credentials_and_security_ids()
        self.assertEqual(result, (None, None))

    async def test_retry_budget_resets_when_a_rejected_session_is_replaced(self):
        wait_for_data = AsyncMock(side_effect=[
            ("first-session", "test-user"),
            ("replacement-session", "test-user"),
            RuntimeError("stop test"),
        ])
        retry_counts = []
        with patch.object(stream, "wait_for_data", wait_for_data), \
             patch.object(stream, "initialize_api"), \
             patch.object(stream, "setup_api_connection", new=AsyncMock(
                 side_effect=stream.BrokerConnectionFailure("transport_error")
             )), \
             patch.object(stream, "_stop_sdk_websocket", new=AsyncMock()), \
             patch.object(stream, "_health_update", side_effect=lambda **updates:
                          retry_counts.append(updates["retryAttempt"])
                          if "retryAttempt" in updates else None), \
             patch.object(stream, "MAX_RETRY_ATTEMPTS", 1):
            with self.assertRaisesRegex(RuntimeError, "stop test"):
                await stream.main(8765)

        self.assertEqual(retry_counts, [0, 1, 0, 1])


class InternalIpcAuthenticationTests(unittest.IsolatedAsyncioTestCase):
    async def _invoke(self, message):
        reader = asyncio.StreamReader()
        reader.feed_data((json.dumps(message) + "\n").encode())
        reader.feed_eof()
        writer = Mock()
        writer.drain = AsyncMock()
        writer.wait_closed = AsyncMock()
        selector = main.BrokerSelector()
        with patch.object(main, "ENV", "production"), \
             patch.object(main, "WEBSOCKET_INTERNAL_TOKEN", "test-internal-token"):
            await selector.handle_client(reader, writer)
        response = json.loads(writer.write.call_args.args[0])
        return selector, response

    async def test_ipc_rejects_missing_internal_token(self):
        selector, response = await self._invoke(
            {"action": "set_broker", "broker": "flattrade"}
        )
        self.assertEqual(response["status"], "error")
        self.assertFalse(selector.broker_selected.is_set())

    async def test_ipc_accepts_authenticated_broker_selection(self):
        selector, response = await self._invoke(
            {
                "action": "set_broker",
                "broker": "flattrade",
                "token": "test-internal-token",
            }
        )
        self.assertEqual(response["status"], "success")
        self.assertTrue(selector.broker_selected.is_set())


if __name__ == "__main__":
    unittest.main()
