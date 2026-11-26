import asyncio
import json
import logging
import random
import threading
import time
from datetime import datetime, timezone

import requests
import websockets
from NorenRestApiPy.NorenApi import NorenApi

from risk_monitor import RiskMonitor

from config import (
    FLATTRADE_RISK_ENDPOINT,
    FLATTRADE_WEBSOCKET_DATA_ENDPOINT,
    WEBSOCKET_INTERNAL_TOKEN,
    WS_ALLOWED_ORIGIN,
    WS_HOST,
)

logging.basicConfig(level=logging.INFO)
logging.getLogger("NorenRestApiPy.NorenApi").setLevel(logging.WARNING)

AUTH_TIMEOUT_SECONDS = 20
MAX_RETRY_ATTEMPTS = 8
MAX_RETRY_DELAY_SECONDS = 30
STABLE_CONNECTION_SECONDS = 60
QUOTE_QUEUE_LIMIT = 2048
HEALTH_PUBLISH_INTERVAL_SECONDS = 5
RISK_SYNC_INTERVAL_SECONDS = 2
RISK_TRIGGER_ATTEMPTS = 5

api = None
loop = None
broker_ready = None
broker_disconnected = None
broker_error_event = None
broker_error_category = None
quote_queue = None
quote_data = {}
quote_data_lock = threading.Lock()
connected_clients = set()
client_subscriptions = {}
subscribed_symbols = set()
risk_monitor = RiskMonitor()
risk_instruments = set()
last_tick_at = None
stream_health = {
    "authenticated": False,
    "connectionState": "starting",
    "localListener": False,
    "lastTickAt": None,
    "lastError": None,
    "retryAttempt": 0,
    "updatedAt": None,
}


class BrokerAuthenticationRejected(RuntimeError):
    pass


class BrokerConnectionFailure(RuntimeError):
    def __init__(self, category):
        super().__init__(category)
        self.category = category


def _health_update(**updates):
    stream_health.update(updates)
    stream_health["updatedAt"] = datetime.now(timezone.utc).isoformat()


def get_stream_health():
    return dict(stream_health)


def _classify_sdk_error(error):
    if isinstance(error, dict) and error.get("t") in ("ck", "ak"):
        if str(error.get("s", "")).upper() != "OK":
            message = str(error.get("emsg", "")).lower()
            if any(word in message for word in ("expired", "invalid", "session", "token")):
                return "session_rejected"
            return "authentication_rejected"
    if isinstance(error, BaseException):
        return "transport_error"
    return "broker_error"


def _record_sdk_error(category):
    global broker_error_category
    broker_error_category = category
    _health_update(
        authenticated=False,
        connectionState="error",
        lastError=category,
    )
    if broker_error_event is not None:
        broker_error_event.set()


def event_handler_order_update(message):
    logging.info("Broker order update received")


def _enqueue_quote(message):
    if quote_queue.full():
        try:
            quote_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        logging.warning("Market-data queue reached its limit; dropped the oldest quote")
    quote_queue.put_nowait(message)


def event_handler_quote_update(message):
    global last_tick_at
    symbol = message.get("tk")
    ltp = message.get("lp")
    if not isinstance(symbol, str) or ltp is None:
        return
    try:
        float(ltp)
    except (TypeError, ValueError):
        return

    last_tick_at = datetime.now(timezone.utc).isoformat()
    with quote_data_lock:
        quote_data[symbol] = ltp
    _health_update(lastTickAt=last_tick_at, lastError=None)
    if loop is not None and not loop.is_closed() and quote_queue is not None:
        loop.call_soon_threadsafe(_enqueue_quote, message)


async def print_quote_data():
    while True:
        await asyncio.sleep(5)
        current_time = time.strftime("%d-%m-%Y %H:%M:%S")
        print(f"\nQuote Data at {current_time}:")
        with quote_data_lock:
            snapshot = list(quote_data.items())
            quote_data.clear()
        for symbol, ltp in snapshot:
            print(f"{symbol}: {ltp}")
        print("------------------------")
        logging.info("stream_health=%s", json.dumps(get_stream_health(), separators=(",", ":")))


async def get_credentials_and_security_ids():
    try:
        response = await asyncio.to_thread(
            requests.get,
            FLATTRADE_WEBSOCKET_DATA_ENDPOINT,
            headers={"X-Steadfast-Internal-Token": WEBSOCKET_INTERNAL_TOKEN},
            timeout=5,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError("Invalid broker session response")
        usersession = data.get("usersession")
        userid = data.get("userid")
        if isinstance(usersession, str) and usersession and isinstance(userid, str) and userid:
            return usersession, userid
        return None, None
    except requests.exceptions.RequestException as error:
        status = error.response.status_code if error.response is not None else None
        logging.warning(
            "Unable to retrieve broker session from API (type=%s, status=%s)",
            type(error).__name__,
            status,
        )
        _health_update(connectionState="waiting_for_session", lastError="session_api_unavailable")
        return None, None
    except ValueError as error:
        logging.warning("Invalid broker session response (type=%s)", type(error).__name__)
        _health_update(connectionState="waiting_for_session", lastError="invalid_session_response")
        return None, None


async def wait_for_data(excluded_session=None):
    _health_update(connectionState="waiting_for_session")
    while True:
        usersession, userid = await get_credentials_and_security_ids()
        if usersession and userid and usersession != excluded_session:
            return usersession, userid
        await asyncio.sleep(5)


def _configure_sdk_single_attempt(api_instance):
    run_method = "_NorenApi__ws_run_forever"
    if not callable(getattr(api_instance, run_method, None)):
        raise RuntimeError("Installed Flattrade SDK WebSocket lifecycle is unsupported")

    def run_once():
        websocket_app = getattr(api_instance, "_NorenApi__websocket", None)
        stop_event = getattr(api_instance, "_NorenApi__stop_event", None)
        if websocket_app is None or stop_event is None or stop_event.is_set():
            return
        try:
            websocket_app.run_forever(ping_interval=3, ping_payload='{"t":"h"}')
        except Exception as error:
            logging.warning("Flattrade SDK transport attempt ended (%s)", type(error).__name__)

    setattr(api_instance, run_method, run_once)


async def _stop_sdk_websocket(api_instance):
    stop_event = getattr(api_instance, "_NorenApi__stop_event", None)
    if stop_event is not None:
        stop_event.set()

    websocket_app = getattr(api_instance, "_NorenApi__websocket", None)
    if websocket_app is not None:
        try:
            websocket_app.close()
        except Exception as error:
            logging.warning("Flattrade SDK socket cleanup failed (%s)", type(error).__name__)

    worker = getattr(api_instance, "_NorenApi__ws_thread", None)
    if worker is not None and worker.is_alive():
        await asyncio.to_thread(worker.join, 5)
        if worker.is_alive():
            logging.error("Flattrade SDK WebSocket worker did not stop within five seconds")


def initialize_api():
    global api
    try:
        api = NorenApi(
            host="https://piconnect.flattrade.in/PiConnectAPI/",
            websocket="wss://piconnect.flattrade.in/PiConnectWSAPI/",
            eodhost="https://web.flattrade.in/chartApi/getdata/",
        )
    except TypeError:
        api = NorenApi(
            host="https://piconnect.flattrade.in/PiConnectAPI/",
            websocket="wss://piconnect.flattrade.in/PiConnectWSAPI/",
        )


def open_callback():
    _health_update(authenticated=True, connectionState="authenticated", lastError=None)
    if broker_ready is not None and loop is not None and not loop.is_closed():
        loop.call_soon_threadsafe(broker_ready.set)
    logging.info("Flattrade broker authentication acknowledged")


def close_callback():
    def mark_disconnected():
        _health_update(authenticated=False, connectionState="disconnected")
        if broker_disconnected is not None:
            broker_disconnected.set()

    if loop is not None and not loop.is_closed():
        loop.call_soon_threadsafe(mark_disconnected)


def error_callback(error):
    category = _classify_sdk_error(error)
    logging.warning("Flattrade WebSocket error category=%s", category)
    if loop is not None and not loop.is_closed():
        loop.call_soon_threadsafe(_record_sdk_error, category)


async def _risk_request(method, path, **kwargs):
    response = await asyncio.to_thread(
        requests.request,
        method,
        f"{FLATTRADE_RISK_ENDPOINT}{path}",
        headers={"X-Steadfast-Internal-Token": WEBSOCKET_INTERNAL_TOKEN},
        timeout=10,
        **kwargs,
    )
    response.raise_for_status()
    return response.json()


async def _notify_reconciliation_after_connect():
    try:
        await _risk_request("POST", "/reconcile")
        logging.info("API reconciliation requested after broker reconnect")
    except (requests.exceptions.RequestException, ValueError) as error:
        logging.warning("API reconciliation reconnect signal failed (type=%s)", type(error).__name__)


async def _send_risk_trigger(rule, reason, ltp):
    payload = {"tsym": rule["tsym"], "rev": rule["rev"], "reason": reason, "ltp": float(ltp)}
    for attempt in range(RISK_TRIGGER_ATTEMPTS):
        try:
            result = await _risk_request("POST", "/trigger", json=payload)
            logging.info("Risk trigger for %s handled=%s status=%s", rule["tsym"], result.get("handled"), result.get("status"))
            return
        except (requests.exceptions.RequestException, ValueError) as error:
            logging.warning("Risk trigger for %s failed (type=%s)", rule["tsym"], type(error).__name__)
            await asyncio.sleep(1)
    # Nothing reached the API, so allow the next tick to request the exit again.
    rule["triggered"] = False


def _process_risk_quote(quote):
    token = quote.get("tk")
    try:
        ltp = float(quote.get("lp"))
    except (TypeError, ValueError):
        return
    if not isinstance(token, str):
        return
    for rule, reason, price in risk_monitor.on_quote(token, ltp):
        logging.info("Risk rule hit for %s: %s at %s", rule["tsym"], reason, price)
        asyncio.get_running_loop().create_task(_send_risk_trigger(rule, reason, price))
    for rule in risk_monitor.rules.values():
        if rule.get("monitor_error") and not rule.get("monitor_error_logged"):
            logging.error("Risk monitor cannot advance %s: %s", rule["tsym"], rule["monitor_error"])
            rule["monitor_error_logged"] = True


def _sync_risk_subscriptions():
    risk_instruments.clear()
    risk_instruments.update(risk_monitor.instruments)
    if not stream_health["authenticated"]:
        if risk_instruments:
            logging.warning("Risk rules are waiting for the broker stream (instruments=%s)", sorted(risk_instruments))
        return
    for instrument in risk_instruments - subscribed_symbols:
        try:
            api.subscribe(instrument=[instrument], feed_type=2)
            subscribed_symbols.add(instrument)
            logging.info("Risk monitor subscribed to %s", instrument)
        except Exception as error:
            logging.warning("Risk subscription failed (%s)", type(error).__name__)


async def _risk_sync_loop():
    last_summary = None
    while True:
        try:
            for rule in risk_monitor.take_dirty():
                result = await _risk_request("POST", "/progress", json={
                    "tsym": rule["tsym"],
                    "rev": rule["rev"],
                    "slPrice": rule["slPrice"],
                    "targetPrice": rule.get("targetPrice"),
                })
                logging.info(
                    "Risk progress pushed for %s: sl=%s target=%s applied=%s",
                    rule["tsym"], rule["slPrice"], rule.get("targetPrice"), result.get("applied"),
                )
            data = await _risk_request("GET", "/rules")
            risk_monitor.sync(data.get("rules", []))
            _sync_risk_subscriptions()
            summary = sorted(
                (rule["tsym"], rule["mode"], rule["slPrice"], rule.get("targetPrice"))
                for rule in risk_monitor.rules.values()
            )
            if summary != last_summary:
                logging.info("Watching %d risk rule(s): %s", len(summary), summary)
                last_summary = summary
        except asyncio.CancelledError:
            raise
        except Exception as error:
            # One bad response must never stop the loop that guards open positions.
            logging.warning("Risk rule sync failed (type=%s)", type(error).__name__)
        await asyncio.sleep(RISK_SYNC_INTERVAL_SECONDS)


async def _broadcast_quote_updates():
    while True:
        quote = await quote_queue.get()
        _process_risk_quote(quote)
        clients = tuple(connected_clients)
        if not clients:
            continue
        payload = json.dumps(quote)
        results = await asyncio.gather(
            *(client.send(payload) for client in clients),
            return_exceptions=True,
        )
        for client, result in zip(clients, results):
            if isinstance(result, Exception):
                connected_clients.discard(client)


async def _broadcast_stream_health():
    while True:
        await asyncio.sleep(HEALTH_PUBLISH_INTERVAL_SECONDS)
        if not connected_clients:
            continue
        payload = json.dumps({"type": "stream_health", "status": get_stream_health()})
        clients = tuple(connected_clients)
        results = await asyncio.gather(
            *(client.send(payload) for client in clients),
            return_exceptions=True,
        )
        for client, result in zip(clients, results):
            if isinstance(result, Exception):
                connected_clients.discard(client)


def _other_client_subscribed(symbol, exclude_client):
    return symbol in risk_instruments or any(
        client is not exclude_client and symbol in symbols
        for client, symbols in client_subscriptions.items()
    )


async def _release_client_subscriptions(websocket):
    symbols = client_subscriptions.pop(websocket, set())
    if not stream_health["authenticated"]:
        return
    for symbol in symbols:
        if _other_client_subscribed(symbol, websocket):
            continue
        try:
            api.unsubscribe(instrument=[symbol], feed_type=2)
            subscribed_symbols.discard(symbol)
        except Exception as error:
            logging.warning("Broker unsubscribe failed (%s)", type(error).__name__)


async def handle_websocket_message(websocket, message):
    data = json.loads(message)
    if not isinstance(data, dict):
        raise ValueError("Invalid WebSocket message")

    action = data.get("action")
    if action not in ("subscribe", "unsubscribe"):
        return

    symbols = data.get("symbols")
    if (
        not isinstance(symbols, list)
        or len(symbols) > 100
        or not all(isinstance(symbol, str) and "|" in symbol for symbol in symbols)
    ):
        logging.warning("Rejected invalid market-data subscription request")
        return

    client_symbols = client_subscriptions.setdefault(websocket, set())
    for symbol in set(symbols):
        if action == "subscribe":
            if symbol in client_symbols:
                continue
            if symbol not in subscribed_symbols:
                api.subscribe(instrument=[symbol], feed_type=2)
                subscribed_symbols.add(symbol)
            client_symbols.add(symbol)
        elif symbol in client_symbols:
            client_symbols.remove(symbol)
            if not _other_client_subscribed(symbol, websocket):
                api.unsubscribe(instrument=[symbol], feed_type=2)
                subscribed_symbols.discard(symbol)


async def websocket_server(websocket):
    connected_clients.add(websocket)
    client_subscriptions[websocket] = set()
    try:
        await websocket.send(json.dumps({"type": "stream_health", "status": get_stream_health()}))
        async for message in websocket:
            await handle_websocket_message(websocket, message)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected_clients.discard(websocket)
        await _release_client_subscriptions(websocket)


async def setup_api_connection(usersession, userid):
    global broker_error_category
    result = api.set_session(userid=userid, password="", usertoken=usersession)
    if result is not True:
        raise BrokerAuthenticationRejected("session_setup_failed")

    _configure_sdk_single_attempt(api)
    broker_ready.clear()
    broker_disconnected.clear()
    broker_error_event.clear()
    broker_error_category = None
    _health_update(connectionState="authenticating", authenticated=False, lastError=None)

    start_result = api.start_websocket(
        order_update_callback=event_handler_order_update,
        subscribe_callback=event_handler_quote_update,
        socket_open_callback=open_callback,
        socket_close_callback=close_callback,
        socket_error_callback=error_callback,
    )
    if start_result is False:
        raise BrokerConnectionFailure("sdk_start_failed")

    ready_task = asyncio.create_task(broker_ready.wait())
    error_task = asyncio.create_task(broker_error_event.wait())
    disconnect_task = asyncio.create_task(broker_disconnected.wait())
    waiters = (ready_task, error_task, disconnect_task)
    try:
        done, _ = await asyncio.wait(
            waiters,
            timeout=AUTH_TIMEOUT_SECONDS,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if error_task in done:
            category = broker_error_category or "broker_error"
            if category in ("session_rejected", "authentication_rejected"):
                raise BrokerAuthenticationRejected(category)
            raise BrokerConnectionFailure(category)
        if disconnect_task in done:
            raise BrokerConnectionFailure("disconnected_before_authentication")
        if ready_task in done:
            return
        raise BrokerConnectionFailure("authentication_ack_timeout")
    finally:
        for task in waiters:
            if not task.done():
                task.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)


async def _serve_authenticated_stream(port):
    connected_clients.clear()
    client_subscriptions.clear()
    subscribed_symbols.clear()

    server = None
    quote_task = None
    health_task = None
    print_task = None
    risk_task = None
    disconnect_task = None
    error_task = None
    server_task = None
    try:
        server = await websockets.serve(
            websocket_server,
            WS_HOST,
            port,
            origins=[WS_ALLOWED_ORIGIN],
        )
        _health_update(localListener=True, connectionState="connected", authenticated=True)
        quote_task = asyncio.create_task(_broadcast_quote_updates())
        health_task = asyncio.create_task(_broadcast_stream_health())
        print_task = asyncio.create_task(print_quote_data())
        risk_task = asyncio.create_task(_risk_sync_loop())
        server_task = asyncio.create_task(server.wait_closed())
        disconnect_task = asyncio.create_task(broker_disconnected.wait())
        error_task = asyncio.create_task(broker_error_event.wait())
        done, _ = await asyncio.wait(
            (server_task, disconnect_task, error_task),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if error_task in done:
            raise BrokerConnectionFailure(broker_error_category or "broker_error")
        if disconnect_task in done:
            raise BrokerConnectionFailure("broker_disconnected")
        raise BrokerConnectionFailure("local_listener_stopped")
    finally:
        _health_update(localListener=False, authenticated=False)
        if server is not None:
            server.close()
            await server.wait_closed()
        for task in (quote_task, health_task, print_task, risk_task, server_task, disconnect_task, error_task):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (quote_task, health_task, print_task, risk_task, server_task, disconnect_task, error_task)
              if task is not None),
            return_exceptions=True,
        )
        connected_clients.clear()
        client_subscriptions.clear()
        subscribed_symbols.clear()


def _retry_delay(attempt):
    base = min(MAX_RETRY_DELAY_SECONDS, 2 ** (attempt - 1))
    return min(MAX_RETRY_DELAY_SECONDS, base * random.uniform(0.75, 1.25))


async def main(port):
    global loop, broker_ready, broker_disconnected, broker_error_event, quote_queue, last_tick_at
    loop = asyncio.get_running_loop()
    broker_ready = asyncio.Event()
    broker_disconnected = asyncio.Event()
    broker_error_event = asyncio.Event()
    quote_queue = asyncio.Queue(maxsize=QUOTE_QUEUE_LIMIT)

    retry_attempt = 0
    rejected_session = None
    while True:
        previous_rejected_session = rejected_session
        usersession, userid = await wait_for_data(previous_rejected_session)
        if previous_rejected_session is not None and usersession != previous_rejected_session:
            retry_attempt = 0
            rejected_session = None
        quote_queue = asyncio.Queue(maxsize=QUOTE_QUEUE_LIMIT)
        last_tick_at = None
        with quote_data_lock:
            quote_data.clear()
        _health_update(lastTickAt=None, retryAttempt=retry_attempt)
        initialize_api()
        authenticated_at = None
        retry_delay = None
        try:
            logging.info("Broker session received from the API")
            await setup_api_connection(usersession, userid)
            await _notify_reconciliation_after_connect()
            authenticated_at = time.monotonic()
            await _serve_authenticated_stream(port)
        except asyncio.CancelledError:
            raise
        except BrokerAuthenticationRejected as error:
            rejected_session = usersession
            retry_attempt = 0
            _health_update(
                authenticated=False,
                localListener=False,
                connectionState="reauthentication_required",
                lastError=error.args[0],
                retryAttempt=0,
            )
            logging.error("Broker authentication rejected; waiting for a refreshed session")
        except BrokerConnectionFailure as error:
            if authenticated_at is not None and time.monotonic() - authenticated_at >= STABLE_CONNECTION_SECONDS:
                retry_attempt = 0
            retry_attempt += 1
            _health_update(
                authenticated=False,
                localListener=False,
                connectionState="reconnecting",
                lastError=error.category,
                retryAttempt=retry_attempt,
            )
            if retry_attempt >= MAX_RETRY_ATTEMPTS:
                rejected_session = usersession
                _health_update(connectionState="retry_limit_reached")
                logging.error("Broker retry limit reached; waiting for a refreshed session")
            else:
                retry_delay = _retry_delay(retry_attempt)
                logging.warning(
                    "Broker connection attempt %s/%s failed (category=%s); retrying after %.1f seconds",
                    retry_attempt,
                    MAX_RETRY_ATTEMPTS,
                    error.category,
                    retry_delay,
                )
        finally:
            await _stop_sdk_websocket(api)
        if retry_delay is not None:
            await asyncio.sleep(retry_delay)
