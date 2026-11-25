import importlib
import os
import unittest
from unittest.mock import patch


class ProductionConfigTests(unittest.TestCase):
    def load_config(self, **values):
        values.setdefault("profile", "prod")
        values.setdefault("WS_ALLOWED_ORIGIN", "https://trade.example.com")
        values.setdefault("WEBSOCKET_INTERNAL_TOKEN", "a" * 32)
        with patch.dict(os.environ, values):
            import config
            return importlib.reload(config)

    def test_production_requires_api_url(self):
        with self.assertRaisesRegex(ValueError, "API_BASE_URL is required"):
            self.load_config(profile="prod", API_BASE_URL="")

    def test_development_profile_uses_local_defaults(self):
        with patch.dict(os.environ, {
            "profile": "dev",
            "WS_ALLOWED_ORIGIN": "",
            "WEBSOCKET_INTERNAL_TOKEN": "",
        }):
            os.environ.pop("API_BASE_URL", None)
            import config
            config = importlib.reload(config)
        self.assertEqual(config.ENV, "development")
        self.assertEqual(config.BASE_URL, "http://127.0.0.1:3000")

    def test_missing_profile_defaults_to_production(self):
        config = self.load_config(
            profile="",
            API_BASE_URL="http://127.0.0.1:3000",
        )
        self.assertEqual(config.ENV, "production")

    def test_explicit_production_profile_overrides_local_development_profile(self):
        config = self.load_config(
            profile="dev",
            PROFILE="prod",
            API_BASE_URL="http://127.0.0.1:3000",
        )
        self.assertEqual(config.ENV, "production")

    def test_invalid_profile_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "profile must be dev or prod"):
            self.load_config(profile="staging")

    def test_repository_dotenv_is_loaded_from_monorepo_root(self):
        with patch("dotenv.load_dotenv") as load_dotenv:
            self.load_config(profile="prod", API_BASE_URL="http://127.0.0.1:3001")
        load_dotenv.assert_called_once()

    def test_loopback_api_and_websocket_host(self):
        config = self.load_config(
            profile="prod",
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
                    self.load_config(profile="prod", API_BASE_URL=url)

    def test_public_websocket_host_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "WS_HOST must be loopback"):
            self.load_config(
                profile="prod",
                API_BASE_URL="http://127.0.0.1:3000",
                WS_HOST="0.0.0.0",
            )

    def test_production_requires_internal_token(self):
        for token in ("", "short", " " * 32, "a" * 257):
            with self.subTest(token=token):
                with self.assertRaisesRegex(ValueError, "WEBSOCKET_INTERNAL_TOKEN"):
                    self.load_config(
                        profile="prod",
                        API_BASE_URL="http://127.0.0.1:3000",
                        WEBSOCKET_INTERNAL_TOKEN=token,
                    )

    def test_production_requires_https_websocket_origin(self):
        for origin in ("", "http://trade.example.com", "https://trade.example.com/path"):
            with self.subTest(origin=origin):
                with self.assertRaisesRegex(ValueError, "WS_ALLOWED_ORIGIN"):
                    self.load_config(
                        profile="prod",
                        API_BASE_URL="http://127.0.0.1:3000",
                        WS_ALLOWED_ORIGIN=origin,
                    )

    def test_invalid_broker_port_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "FLATTRADE_WS_PORT"):
            self.load_config(
                profile="prod",
                API_BASE_URL="http://127.0.0.1:3000",
                FLATTRADE_WS_PORT="65536",
            )


if __name__ == "__main__":
    unittest.main()
