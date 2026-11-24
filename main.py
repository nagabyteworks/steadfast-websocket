import os
import asyncio
import hmac
import json
from config import ENV, FLATTRADE_WS_PORT, SHOONYA_WS_PORT, WEBSOCKET_INTERNAL_TOKEN
from flattrade.flattrade_websocket import (
    main as flattrade_main,
    initialize_api as flattrade_initialize_api,
)
from shoonya.shoonya_websocket import (
    main as shoonya_main,
    initialize_api as shoonya_initialize_api,
)

# Define broker-specific ports
BROKER_PORTS = {"flattrade": FLATTRADE_WS_PORT, "shoonya": SHOONYA_WS_PORT}

IPC_PORT = 5000  # Same port as in Node.js server


class BrokerSelector:
    def __init__(self):
        self.selected_broker = None
        self.broker_selected = asyncio.Event()
        print("BrokerSelector initialized")

    async def handle_client(self, reader, writer):
        try:
            data = await asyncio.wait_for(reader.readline(), timeout=5)
            if not data or len(data) > 2048:
                raise ValueError("Invalid IPC message")
            parsed_message = json.loads(data.decode())
            if not isinstance(parsed_message, dict):
                raise ValueError("Invalid broker selection")
            provided_token = parsed_message.pop("token", None)
            if (
                ENV == "production" or WEBSOCKET_INTERNAL_TOKEN
            ) and (
                not isinstance(provided_token, str)
                or not hmac.compare_digest(provided_token, WEBSOCKET_INTERNAL_TOKEN)
            ):
                writer.write(json.dumps({"status": "error", "message": "IPC authentication failed"}).encode())
                await writer.drain()
                return
            broker = parsed_message.get("broker")
            allowed_brokers = {"flattrade"} if ENV == "production" else BROKER_PORTS
            if parsed_message.get("action") == "set_broker" and broker in allowed_brokers:
                if broker != self.selected_broker or parsed_message.get("restart") is True:
                    self.selected_broker = broker
                    self.broker_selected.set()
                response = json.dumps(
                    {
                        "status": "success",
                        "message": f"Broker set to {self.selected_broker}",
                    }
                )
                writer.write(response.encode())
                await writer.drain()
            else:
                writer.write(json.dumps(
                    {"status": "error", "message": "Invalid broker selection"}
                ).encode())
                await writer.drain()
        except (asyncio.TimeoutError, json.JSONDecodeError, UnicodeDecodeError, ValueError):
            writer.write(json.dumps({"status": "error", "message": "Invalid IPC message"}).encode())
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def start_ipc_server(self):
        server = await asyncio.start_server(
            self.handle_client,
            "127.0.0.1",
            IPC_PORT,
            limit=2048,
        )
        addr = server.sockets[0].getsockname()
        print(f"IPC Server running on {addr}")

        async with server:
            await server.serve_forever()


async def main():
    broker_selector = BrokerSelector()
    if ENV == "production":
        broker_selector.selected_broker = "flattrade"
        broker_selector.broker_selected.set()
    ipc_server_task = asyncio.create_task(broker_selector.start_ipc_server())
    selection_task = None
    websocket_task = None
    selected_broker = None

    try:
        print("WebSocket server starting...")
        print("Waiting for broker selection...")
        while True:
            selection_task = asyncio.create_task(broker_selector.broker_selected.wait())
            tasks = [ipc_server_task, selection_task]
            if websocket_task is not None:
                tasks.append(websocket_task)
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if selection_task in done:
                broker_selector.broker_selected.clear()
                if websocket_task is not None:
                    websocket_task.cancel()
                    await asyncio.gather(websocket_task, return_exceptions=True)

                selected_broker = broker_selector.selected_broker
                if selected_broker not in BROKER_PORTS:
                    raise ValueError(f"Invalid broker selection: {selected_broker}")
                print(f"Selected broker: {selected_broker}")
                ws_port = BROKER_PORTS[selected_broker]
                print(f"Starting {selected_broker.capitalize()} WebSocket on port {ws_port}...")
                if selected_broker == "flattrade":
                    flattrade_initialize_api()
                    websocket_task = asyncio.create_task(flattrade_main(ws_port))
                elif selected_broker == "shoonya":
                    shoonya_initialize_api()
                    websocket_task = asyncio.create_task(shoonya_main(ws_port))
                continue

            if ipc_server_task in done:
                await ipc_server_task
                raise RuntimeError("IPC server stopped unexpectedly")
            if websocket_task is not None and websocket_task in done:
                await websocket_task
                raise RuntimeError("Broker WebSocket supervisor stopped unexpectedly")
    finally:
        tasks = [ipc_server_task]
        if selection_task is not None:
            tasks.append(selection_task)
        if websocket_task is not None:
            tasks.append(websocket_task)
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server shutting down...")
