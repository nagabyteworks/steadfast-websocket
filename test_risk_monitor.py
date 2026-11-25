import unittest

from risk_monitor import RiskMonitor, evaluate


def rule(**overrides):
    base = {
        "tsym": "NIFTYCE", "instrument": "NFO|123", "side": "long", "entry": 100.0,
        "mode": "static", "slPrice": 90.0, "slPoints": None, "targetPrice": None,
        "trailPoints": None, "rev": 1,
    }
    base.update(overrides)
    return base


class EvaluateTests(unittest.TestCase):
    def test_target_trail_advances_stop_without_exiting(self):
        r = rule(mode="target_trail", targetPrice=120.0, trailPoints=5.0)
        self.assertEqual(evaluate(r, 119), (False, None))
        self.assertEqual(evaluate(r, 120), (True, None))
        self.assertEqual((r["slPrice"], r["targetPrice"]), (120.0, 125.0))
        self.assertEqual(evaluate(r, 126), (True, None))
        self.assertEqual((r["slPrice"], r["targetPrice"]), (125.0, 130.0))
        self.assertEqual(evaluate(r, 125), (False, "Stop-loss"))

    def test_target_trail_skips_milestones_on_a_price_gap(self):
        r = rule(mode="target_trail", targetPrice=120.0, trailPoints=5.0)
        evaluate(r, 131)
        self.assertEqual((r["slPrice"], r["targetPrice"]), (130.0, 135.0))

    def test_target_trail_for_short_positions(self):
        r = rule(side="short", entry=100.0, slPrice=110.0, mode="target_trail", targetPrice=80.0, trailPoints=5.0)
        evaluate(r, 80)
        self.assertEqual((r["slPrice"], r["targetPrice"]), (80.0, 75.0))
        self.assertEqual(evaluate(r, 80), (False, "Stop-loss"))

    def test_static_stop_and_target_exit(self):
        r = rule(targetPrice=120.0)
        self.assertEqual(evaluate(r, 90), (False, "Stop-loss"))
        self.assertEqual(evaluate(r, 120), (False, "Target"))

    def test_trailing_stop_follows_price_and_never_loosens(self):
        r = rule(mode="trailing_sl", slPoints=10.0)
        self.assertEqual(evaluate(r, 115), (True, None))
        self.assertEqual(r["slPrice"], 105.0)
        self.assertEqual(evaluate(r, 110), (False, None))
        self.assertEqual(r["slPrice"], 105.0)
        self.assertEqual(evaluate(r, 105), (False, "Stop-loss"))


    def test_rule_without_stop_loss_only_uses_target(self):
        r = rule(slPrice=None, targetPrice=120.0)
        self.assertEqual(evaluate(r, 50), (False, None))
        self.assertEqual(evaluate(r, 121), (False, "Target"))
        t = rule(slPrice=None, mode="target_trail", targetPrice=120.0, trailPoints=5.0)
        evaluate(t, 121)
        self.assertEqual(t["slPrice"], 120.0)


class MonitorTests(unittest.TestCase):
    def test_rule_triggers_once_and_keeps_local_progress_for_same_rev(self):
        monitor = RiskMonitor()
        monitor.sync([rule(mode="target_trail", targetPrice=120.0, trailPoints=5.0)])
        monitor.on_quote("123", 121.0)
        self.assertEqual(len(monitor.take_dirty()), 1)
        monitor.sync([rule(mode="target_trail", targetPrice=120.0, trailPoints=5.0)])
        self.assertEqual(monitor.rules["NIFTYCE"]["slPrice"], 120.0)
        self.assertEqual(len(monitor.on_quote("123", 119.0)), 1)
        self.assertEqual(monitor.on_quote("123", 118.0), [])

    def test_user_edit_replaces_local_state(self):
        monitor = RiskMonitor()
        monitor.sync([rule()])
        monitor.sync([rule(slPrice=95.0, rev=2)])
        self.assertEqual(monitor.rules["NIFTYCE"]["slPrice"], 95.0)


if __name__ == "__main__":
    unittest.main()
