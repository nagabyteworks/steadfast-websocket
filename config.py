import os
import re
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

# Determine the environment
ENV = os.getenv("ENV")

if ENV == "production":
    BASE_URL = os.getenv("API_BASE_URL")
    if not BASE_URL:
        raise ValueError("API_BASE_URL is required in production")
elif ENV == "development":
    BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:3000")
else:
    raise ValueError(f"Invalid environment: {ENV}")

api_url = urlparse(BASE_URL)
if (
    api_url.scheme not in ("http", "https")
    or not api_url.hostname
    or api_url.username is not None
    or api_url.password is not None
    or api_url.path not in ("", "/")
    or api_url.query
    or api_url.fragment
):
    raise ValueError("API_BASE_URL must be an absolute HTTP or HTTPS origin")
if ENV == "production" and api_url.hostname not in ("localhost", "127.0.0.1", "::1"):
    raise ValueError("Production API_BASE_URL must use a loopback address")

WEBSOCKET_INTERNAL_TOKEN = os.getenv("WEBSOCKET_INTERNAL_TOKEN", "")
if ENV == "production" and not WEBSOCKET_INTERNAL_TOKEN:
    raise ValueError("WEBSOCKET_INTERNAL_TOKEN is required in production")
if WEBSOCKET_INTERNAL_TOKEN and not re.fullmatch(r"[A-Za-z0-9_-]{32,256}", WEBSOCKET_INTERNAL_TOKEN):
    raise ValueError("WEBSOCKET_INTERNAL_TOKEN must be 32-256 URL-safe characters")

BASE_URL = BASE_URL.rstrip("/")
def read_port(name, default):
    port = int(os.getenv(name, default))
    if not 1 <= port <= 65535:
        raise ValueError(f"{name} must be an integer between 1 and 65535")
    return port


FLATTRADE_WS_PORT = read_port("FLATTRADE_WS_PORT", 8765)
SHOONYA_WS_PORT = read_port("SHOONYA_WS_PORT", 8766)

# API Endpoints
FLATTRADE_WEBSOCKET_DATA_ENDPOINT = f"{BASE_URL}/flattrade/websocketData"
SHOONYA_WEBSOCKET_DATA_ENDPOINT = f"{BASE_URL}/shoonya/websocketData"

# WebSocket server settings
WS_HOST = os.getenv("WS_HOST", "127.0.0.1")
if ENV == "production" and WS_HOST not in ("127.0.0.1", "::1", "localhost"):
    raise ValueError("WS_HOST must be loopback in production")
WS_ALLOWED_ORIGIN = os.getenv("WS_ALLOWED_ORIGIN")
if ENV == "production" and not WS_ALLOWED_ORIGIN:
    raise ValueError("WS_ALLOWED_ORIGIN is required in production")
if WS_ALLOWED_ORIGIN:
    origin = urlparse(WS_ALLOWED_ORIGIN)
    if (
        origin.scheme not in ("http", "https")
        or not origin.hostname
        or WS_ALLOWED_ORIGIN != f"{origin.scheme}://{origin.netloc}"
        or origin.username is not None
        or origin.password is not None
        or (ENV == "production" and origin.scheme != "https")
    ):
        raise ValueError("WS_ALLOWED_ORIGIN must be an HTTPS origin in production")
