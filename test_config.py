import importlib
import os
import unittest
from unittest.mock import patch


class ProductionConfigTests(unittest.TestCase):
    def load_config(self, **values):
        values.setdefault("WS_ALLOWED_ORIGIN", "https://trade.example.com")
        values.setdefault("WEBSOCKET_INTERNAL_TOKEN", "a" * 32)
        with patch.dict(os.environ, values):
            import config
            return importlib.reload(config)

    def test_production_requires_api_url(self):
        with self.assertRaisesRegex(ValueError, "API_BASE_URL is required"):
            self.load_config(ENV="production", API_BASE_URL="")

    def test_environment_is_required(self):
        with self.assertRaisesRegex(ValueError, "Invalid environment"):
            self.load_config(ENV="")

    def test_production_does_not_load_repository_dotenv(self):
        with patch("dotenv.load_dotenv") as load_dotenv:
            self.load_config(
                ENV="production",
                API_BASE_URL="http://127.0.0.1:3001",
            )
        load_dotenv.assert_not_called()

    def test_loopback_api_and_websocket_host(self):
        config = self.load_config(
            ENV="production",
            API_BASE_URL="http://127.0.0.1:3001/",
            WS_HOST="127.0.0.1",
            FLATTRADE_WS_PORT="8765",
        )
        self.assertEqual(config.FLATTRADE_WEBSOCKET_DATA_ENDPOINT,
                         "http://127.0.0.1:3001/flattrade/websocketData")
        self.assertEqual(config.WS_HOST, "127.0.0.1")

    def test_remote_api_is_rejected(self):
        for url in ("http://example.com", "https://example.com"):
            with self.subTest(url=url):
                with self.assertRaisesRegex(ValueError, "loopback"):
                    self.load_config(ENV="production", API_BASE_URL=url)

    def test_public_websocket_host_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "WS_HOST must be loopback"):
            self.load_config(
                ENV="production",
                API_BASE_URL="http://127.0.0.1:3000",
                WS_HOST="0.0.0.0",
            )

    def test_production_requires_internal_token(self):
        for token in ("", "short", " " * 32, "a" * 257):
            with self.subTest(token=token):
                with self.assertRaisesRegex(ValueError, "WEBSOCKET_INTERNAL_TOKEN"):
                    self.load_config(
                        ENV="production",
                        API_BASE_URL="http://127.0.0.1:3000",
                        WEBSOCKET_INTERNAL_TOKEN=token,
                    )

    def test_production_requires_https_websocket_origin(self):
        for origin in ("", "http://trade.example.com", "https://trade.example.com/path"):
            with self.subTest(origin=origin):
                with self.assertRaisesRegex(ValueError, "WS_ALLOWED_ORIGIN"):
                    self.load_config(
                        ENV="production",
                        API_BASE_URL="http://127.0.0.1:3000",
                        WS_ALLOWED_ORIGIN=origin,
                    )

    def test_invalid_broker_port_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "FLATTRADE_WS_PORT"):
            self.load_config(
                ENV="production",
                API_BASE_URL="http://127.0.0.1:3000",
                FLATTRADE_WS_PORT="65536",
            )


if __name__ == "__main__":
    unittest.main()
