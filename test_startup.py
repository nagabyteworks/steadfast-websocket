import asyncio
import inspect
import os
import unittest
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault("ENV", "development")

import main
from NorenRestApiPy.NorenApi import NorenApi
from flattrade import flattrade_websocket
from shoonya import shoonya_websocket


class StartupFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_production_session_change_exits_for_supervised_restart(self):
        selector = main.BrokerSelector()
        first_started = asyncio.Event()
        first_stopped = asyncio.Event()

        async def ipc_server():
            await asyncio.Event().wait()

        async def broker_server(port):
            first_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                first_stopped.set()

        with patch.object(main, "ENV", "production"), \
             patch.object(main, "BrokerSelector", return_value=selector), \
             patch.object(selector, "start_ipc_server", side_effect=ipc_server), \
             patch.object(main, "flattrade_initialize_api"), \
             patch.object(main, "flattrade_main", side_effect=broker_server):
            task = asyncio.create_task(main.main())
            await asyncio.wait_for(first_started.wait(), timeout=1)
            selector.broker_selected.set()
            with self.assertRaisesRegex(RuntimeError, "supervisor must restart"):
                await asyncio.wait_for(task, timeout=1)
        self.assertTrue(first_stopped.is_set())

    async def test_flattrade_sdk_session_interface_and_authentication_ack(self):
        self.assertEqual(list(inspect.signature(NorenApi.set_session).parameters),
                         ["self", "userid", "password", "usertoken"])
        api = Mock()
        api.set_session.return_value = True
        api.start_websocket.side_effect = lambda **callbacks: callbacks["socket_open_callback"]()
        ready = asyncio.Event()
        disconnected = asyncio.Event()
        with patch.object(flattrade_websocket, "api", api, create=True), \
             patch.object(flattrade_websocket, "loop", asyncio.get_running_loop(), create=True), \
             patch.object(flattrade_websocket, "broker_ready", ready), \
             patch.object(flattrade_websocket, "broker_disconnected", disconnected):
            await flattrade_websocket.setup_api_connection("fake-session", "fake-user")
        api.set_session.assert_called_once_with(userid="fake-user", password="", usertoken="fake-session")
        self.assertTrue(ready.is_set())

    async def test_flattrade_does_not_expose_stream_without_broker_ack(self):
        api = Mock()
        api.set_session.return_value = True
        api.start_websocket.return_value = None
        with patch.object(flattrade_websocket, "api", api, create=True), \
             patch.object(flattrade_websocket, "broker_ready", asyncio.Event()), \
             patch.object(flattrade_websocket, "AUTH_TIMEOUT_SECONDS", 0.01):
            with self.assertRaisesRegex(RuntimeError, "authentication was not acknowledged"):
                await flattrade_websocket.setup_api_connection("fake-session", "fake-user")

    async def test_same_broker_restart_replaces_the_running_stream(self):
        selector = main.BrokerSelector()
        selector.selected_broker = "flattrade"
        selector.broker_selected.set()
        first_started = asyncio.Event()
        first_stopped = asyncio.Event()
        calls = 0

        async def ipc_server():
            await asyncio.Event().wait()

        async def broker_server(port):
            nonlocal calls
            calls += 1
            if calls == 1:
                first_started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    first_stopped.set()
            else:
                self.assertTrue(first_stopped.is_set())
                raise RuntimeError("replacement started")

        with patch.object(main, "BrokerSelector", return_value=selector), \
             patch.object(selector, "start_ipc_server", side_effect=ipc_server), \
             patch.object(main, "flattrade_initialize_api"), \
             patch.object(main, "flattrade_main", side_effect=broker_server):
            task = asyncio.create_task(main.main())
            await asyncio.wait_for(first_started.wait(), timeout=1)
            selector.broker_selected.set()
            with self.assertRaisesRegex(RuntimeError, "replacement started"):
                await asyncio.wait_for(task, timeout=1)
        self.assertEqual(calls, 2)

    async def test_ipc_start_failure_exits_instead_of_waiting_for_broker(self):
        with patch.object(
            main.BrokerSelector,
            "start_ipc_server",
            new=AsyncMock(side_effect=OSError("IPC unavailable")),
        ):
            with self.assertRaisesRegex(OSError, "IPC unavailable"):
                await asyncio.wait_for(main.main(), timeout=1)

    async def test_websocket_startup_failure_is_not_swallowed(self):
        for module in (flattrade_websocket, shoonya_websocket):
            with self.subTest(broker=module.__name__):
                with patch.object(
                    module,
                    "wait_for_data",
                    new=AsyncMock(side_effect=RuntimeError("session unavailable")),
                ):
                    with self.assertRaisesRegex(RuntimeError, "session unavailable"):
                        await module.main(8765)

    async def test_broker_service_uses_configured_port_and_reports_unexpected_stop(self):
        selector = main.BrokerSelector()
        selector.selected_broker = "flattrade"
        selector.broker_selected.set()
        ipc_started = asyncio.Event()
        ipc_cancelled = asyncio.Event()

        async def ipc_server():
            ipc_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                ipc_cancelled.set()

        async def broker_server(port):
            await ipc_started.wait()

        with patch.object(main, "BrokerSelector", return_value=selector), \
             patch.object(selector, "start_ipc_server", side_effect=ipc_server), \
             patch.object(main, "flattrade_initialize_api"), \
             patch.object(main, "flattrade_main", side_effect=broker_server) as broker:
            with self.assertRaisesRegex(RuntimeError, "stopped unexpectedly"):
                await asyncio.wait_for(main.main(), timeout=1)

        broker.assert_awaited_once_with(main.BROKER_PORTS["flattrade"])
        self.assertTrue(ipc_cancelled.is_set())

    async def test_broker_websocket_explicit_failure_is_reported(self):
        for module in (flattrade_websocket, shoonya_websocket):
            with self.subTest(broker=module.__name__):
                api = Mock()
                api.set_session.return_value = True
                api.start_websocket.return_value = False
                with patch.object(module, "api", api, create=True):
                    with self.assertRaisesRegex(RuntimeError, "did not start"):
                        await module.setup_api_connection("test-session", "test-user")

    async def test_broker_listeners_restrict_the_websocket_origin(self):
        for module in (flattrade_websocket, shoonya_websocket):
            with self.subTest(broker=module.__name__):
                server = Mock()
                server.wait_closed = AsyncMock()
                with patch.object(module, "WS_ALLOWED_ORIGIN", "https://trade.example.com"), \
                     patch.object(module, "wait_for_data", new=AsyncMock(return_value=("session", "user"))), \
                     patch.object(module, "setup_api_connection", new=AsyncMock()), \
                     patch.object(module, "api", Mock(), create=True), \
                     patch.object(module.websockets, "serve", new=AsyncMock(return_value=server)) as serve:
                    await module.main(8765)
                self.assertEqual(serve.await_args.kwargs["origins"], ["https://trade.example.com"])
                server.close.assert_called_once()

    async def test_credential_requests_use_internal_token(self):
        for module in (flattrade_websocket, shoonya_websocket):
            with self.subTest(broker=module.__name__):
                response = Mock()
                response.json.return_value = {"usersession": "fake-session", "userid": "fake-user"}
                with patch.object(module, "WEBSOCKET_INTERNAL_TOKEN", "a" * 32), \
                     patch.object(module.requests, "get", return_value=response) as get:
                    self.assertEqual(
                        await module.get_credentials_and_security_ids(),
                        ("fake-session", "fake-user"),
                    )
                self.assertEqual(get.call_args.kwargs["headers"]["X-Steadfast-Internal-Token"], "a" * 32)
                self.assertEqual(get.call_args.kwargs["timeout"], 5)


if __name__ == "__main__":
    unittest.main()
