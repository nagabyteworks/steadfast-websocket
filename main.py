import os
import asyncio
import json
from config import ENV, FLATTRADE_WS_PORT, SHOONYA_WS_PORT
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
        print("New client connection received")
        data = await reader.read(100)
        message = data.decode()
        addr = writer.get_extra_info("peername")

        print(f"Broker selection request received from {addr!r}")

        try:
            parsed_message = json.loads(message)
            if not isinstance(parsed_message, dict):
                raise ValueError("Invalid broker selection")
            broker = parsed_message.get("broker")
            if parsed_message.get("action") == "set_broker" and broker in BROKER_PORTS:
                if broker != self.selected_broker or parsed_message.get("restart") is True:
                    self.selected_broker = broker
                    self.broker_selected.set()
                print(f"Broker set to: {self.selected_broker}")
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
        except (json.JSONDecodeError, ValueError):
            print("Received invalid JSON")
            response = json.dumps(
                {"status": "error", "message": "Invalid JSON received"}
            )
            writer.write(response.encode())
            await writer.drain()

        writer.close()
        await writer.wait_closed()

    async def start_ipc_server(self):
        server = await asyncio.start_server(
            self.handle_client,
            "127.0.0.1",
            IPC_PORT,
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

    try:
        print("WebSocket server starting...")
        print("Waiting for broker selection...")
        while True:
            selection_task = asyncio.create_task(broker_selector.broker_selected.wait())
            tasks = [ipc_server_task, selection_task]
            if websocket_task is not None:
                tasks.append(websocket_task)
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in (ipc_server_task, websocket_task):
                if task is not None and task in done:
                    await task
                    raise RuntimeError("IPC or broker WebSocket server stopped unexpectedly")

            broker_selector.broker_selected.clear()
            if websocket_task is not None:
                if ENV == "production":
                    raise RuntimeError("Broker session changed; supervisor must restart the streaming process")
                websocket_task.cancel()
                try:
                    await websocket_task
                except asyncio.CancelledError:
                    pass

            selected_broker = broker_selector.selected_broker
            print(f"Selected broker: {selected_broker}")
            ws_port = BROKER_PORTS[selected_broker]
            print(f"Starting {selected_broker.capitalize()} WebSocket on port {ws_port}...")
            if selected_broker == "flattrade":
                flattrade_initialize_api()
                websocket_task = asyncio.create_task(flattrade_main(ws_port))
            elif selected_broker == "shoonya":
                shoonya_initialize_api()
                websocket_task = asyncio.create_task(shoonya_main(ws_port))
            else:
                raise ValueError(f"Invalid broker selection: {selected_broker}")
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
