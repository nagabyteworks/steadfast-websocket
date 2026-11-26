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
    def test_reopened_long_price_paths(self):
        fixed = rule(entry=150, slPrice=140, targetPrice=170)
        self.assertEqual(evaluate(fixed, 140.01), (False, None))
        self.assertEqual(evaluate(fixed, 139), (False, "Stop-loss"))
        self.assertEqual(evaluate(fixed, 169.99), (False, None))
        self.assertEqual(evaluate(fixed, 171), (False, "Target"))

        trailing = rule(entry=150, slPrice=140, mode="trailing_sl", slPoints=10)
        self.assertEqual(evaluate(trailing, 165), (True, None))
        self.assertEqual(trailing["slPrice"], 155)
        self.assertEqual(evaluate(trailing, 160), (False, None))
        self.assertEqual(trailing["slPrice"], 155)
        self.assertEqual(evaluate(trailing, 155), (False, "Stop-loss"))

        target_trail = rule(entry=150, slPrice=140, mode="target_trail", targetPrice=170, trailPoints=5)
        self.assertEqual(evaluate(target_trail, 169.99), (False, None))
        self.assertEqual(evaluate(target_trail, 181), (True, None))
        self.assertEqual((target_trail["slPrice"], target_trail["targetPrice"]), (180, 185))
        self.assertEqual(evaluate(target_trail, 180), (False, "Stop-loss"))

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

    def test_static_stop_and_target_for_short_positions(self):
        r = rule(side="short", slPrice=110.0, targetPrice=80.0)
        self.assertEqual(evaluate(r, 100), (False, None))
        self.assertEqual(evaluate(r, 110), (False, "Stop-loss"))
        self.assertEqual(evaluate(r, 80), (False, "Target"))

    def test_static_rule_never_moves_its_stop(self):
        r = rule(targetPrice=120.0)
        self.assertEqual(evaluate(r, 115), (False, None))
        self.assertEqual(r["slPrice"], 90.0)

    def test_trailing_stop_for_short_positions(self):
        r = rule(side="short", slPrice=110.0, mode="trailing_sl", slPoints=10.0)
        self.assertEqual(evaluate(r, 85), (True, None))
        self.assertEqual(r["slPrice"], 95.0)
        self.assertEqual(evaluate(r, 90), (False, None))
        self.assertEqual(r["slPrice"], 95.0)
        self.assertEqual(evaluate(r, 95), (False, "Stop-loss"))

    def test_trailing_stop_still_exits_at_target(self):
        r = rule(mode="trailing_sl", slPoints=10.0, targetPrice=120.0)
        self.assertEqual(evaluate(r, 120), (True, "Target"))

    def test_target_trail_does_not_move_before_target(self):
        r = rule(mode="target_trail", targetPrice=120.0, trailPoints=5.0)
        self.assertEqual(evaluate(r, 110), (False, None))
        self.assertEqual((r["slPrice"], r["targetPrice"]), (90.0, 120.0))


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

    def test_trailing_progress_is_reported_once_and_ignores_other_tokens(self):
        monitor = RiskMonitor()
        monitor.sync([rule(mode="trailing_sl", slPoints=10.0)])
        self.assertEqual(monitor.on_quote("999", 150.0), [])
        monitor.on_quote("123", 115.0)
        self.assertEqual(monitor.take_dirty()[0]["slPrice"], 105.0)
        self.assertEqual(monitor.take_dirty(), [])

    def test_triggered_rule_is_not_reported_as_dirty(self):
        monitor = RiskMonitor()
        monitor.sync([rule(mode="trailing_sl", slPoints=10.0, targetPrice=120.0)])
        self.assertEqual(len(monitor.on_quote("123", 125.0)), 1)
        self.assertEqual(monitor.take_dirty(), [])


if __name__ == "__main__":
    unittest.main()
