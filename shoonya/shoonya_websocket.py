import asyncio
import websockets
import json
import logging
from config import SHOONYA_WEBSOCKET_DATA_ENDPOINT, WS_HOST, WS_ALLOWED_ORIGIN, WEBSOCKET_INTERNAL_TOKEN
from NorenRestApiPy.NorenApi import NorenApi
import requests
import time

logging.basicConfig(level=logging.INFO)

# Flag to tell us if the websocket is open
socket_opened = False


# Event handlers
def event_handler_order_update(message):
    logging.info("Broker order update received")


def event_handler_quote_update(message):
    global quote_data
    symbol = message.get("tk", "Unknown")
    ltp = message.get("lp", "N/A")
    quote_data[symbol] = ltp

    # Remove the asyncio.run_coroutine_threadsafe and directly put to queue
    loop.call_soon_threadsafe(quote_queue.put_nowait, message)


quote_data = {}
PRINT_INTERVAL = 5  # Print every 5 seconds


async def print_quote_data():
    while True:
        await asyncio.sleep(PRINT_INTERVAL)
        current_time = time.strftime("%d-%m-%Y %H:%M:%S")
        print(f"\nQuote Data at {current_time}:")
        for symbol, ltp in quote_data.items():
            print(f"{symbol}: {ltp}")
        print("------------------------")
        quote_data.clear()


async def get_credentials_and_security_ids():
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: requests.get(
                SHOONYA_WEBSOCKET_DATA_ENDPOINT,
                headers={"X-Steadfast-Internal-Token": WEBSOCKET_INTERNAL_TOKEN}
                if WEBSOCKET_INTERNAL_TOKEN else {},
                timeout=5,
            ),
        )
        response.raise_for_status()
        data = response.json()
        usersession = data.get("usersession", "")
        userid = data.get("userid", "")

        if usersession and userid:
            logging.info("Valid data retrieved successfully")
            return usersession, userid
        else:
            logging.info("Waiting for valid data...")
            return None, None
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to retrieve data: {e}")
        return None, None


async def wait_for_data():
    while True:
        usersession, userid = await get_credentials_and_security_ids()
        if usersession and userid:
            return usersession, userid
        await asyncio.sleep(5)


async def setup_api_connection(usersession, userid):
    global api
    # Set up the session
    ret = api.set_session(userid=userid, password="", usertoken=usersession)

    if ret is not None:
        # Start the websocket
        result = api.start_websocket(
            order_update_callback=event_handler_order_update,
            subscribe_callback=event_handler_quote_update,
            socket_open_callback=open_callback,  # No parameters needed
        )
        if result is False:
            raise RuntimeError("Broker WebSocket did not start")
    else:
        raise Exception("Failed to set up API session")


def open_callback():
    global socket_opened
    socket_opened = True
    print("app is connected")

    # Optionally, you can log or perform other actions here
    # without subscribing to hardcoded symbols


quote_queue = asyncio.Queue()


async def websocket_server(websocket):
    try:
        # Create a task to continuously send quote updates to the client
        send_task = asyncio.create_task(send_quote_updates(websocket))

        async for message in websocket:
            await handle_websocket_message(websocket, message)
    except websockets.exceptions.ConnectionClosed:
        print("Connection closed")
    finally:
        # Cancel the send task when the connection is closed
        send_task.cancel()


async def send_quote_updates(websocket):
    while True:
        try:
            quote = await quote_queue.get()
            await websocket.send(json.dumps(quote))
        except Exception as e:
            logging.error(f"Error sending quote update: {e}")
            # If there's an error, wait a bit before trying again
            await asyncio.sleep(1)


async def handle_websocket_message(websocket, message):
    data = json.loads(message)
    if "action" in data:
        if data["action"] == "unsubscribe":
            for symbol in data["symbols"]:
                api.unsubscribe(instrument=[symbol], feed_type=2)
                print(f"\nUnsubscribed from {symbol}")
                # logging.info(f"Unsubscribed from {symbol}")
        elif data["action"] == "subscribe":
            for symbol in data["symbols"]:
                api.subscribe(instrument=[symbol], feed_type=2)
                print(f"\nSubscribed to {symbol}")
                # logging.info(f"Subscribed to {symbol}")

            # Add a small delay after subscribing
            await asyncio.sleep(0.1)

            # Check for any pending quote updates
            while not quote_queue.empty():
                quote = await quote_queue.get()
                await websocket.send(json.dumps(quote))
    else:
        # Handle the existing credential update logic
        global usersession, userid
        usersession = data.get("usersession", "")
        userid = data.get("userid", "")
        print("Updated credentials")


async def main(port):
    global loop
    loop = asyncio.get_running_loop()

    print_task = None
    server = None
    session_started = False
    try:
        logging.info(f"Starting Shoonya WebSocket on port {port}...")
        usersession, userid = await wait_for_data()
        logging.info("Broker session received")
        session_started = True
        await setup_api_connection(usersession, userid)

        # Create the print_quote_data task
        print_task = asyncio.create_task(print_quote_data())

        server = await websockets.serve(
            websocket_server, WS_HOST, port,
            origins=[WS_ALLOWED_ORIGIN] if WS_ALLOWED_ORIGIN else None,
        )
        await server.wait_closed()
    except Exception as e:
        logging.error(f"An error occurred in Shoonya WebSocket: {e}")
        raise
    finally:
        if server is not None:
            server.close()
            await server.wait_closed()
        if print_task is not None:
            print_task.cancel()
            await asyncio.gather(print_task, return_exceptions=True)
        if session_started:
            await asyncio.to_thread(api.close_websocket)


def initialize_api():
    global api
    try:
        api = NorenApi(
            host="https://api.shoonya.com/NorenWClientTP/",
            websocket="wss://api.shoonya.com/NorenWSTP/",
            eodhost="https://api.shoonya.com/chartApi/getdata/",
        )
    except TypeError:
        # If 'eodhost' is not accepted, try without it
        api = NorenApi(
            host="https://api.shoonya.com/NorenWClientTP/",
            websocket="wss://api.shoonya.com/NorenWSTP/",
        )


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        from config import SHOONYA_WS_PORT

        port = SHOONYA_WS_PORT
    asyncio.run(main(port))
