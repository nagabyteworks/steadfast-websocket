"""Tracks stop-loss / target rules against live prices.

The API owns the rules and places exit orders. This module only decides, tick by
tick, whether a rule's stop-loss moved or an exit must be requested.
"""

import math


def _sign(rule):
    return 1 if rule["side"] == "long" else -1


def evaluate(rule, ltp):
    """Applies one price to a rule. Returns (changed, exit_reason or None).

    Modes:
      static        - fixed stop-loss, optional target that exits.
      trailing_sl   - stop-loss follows the price by slPoints, optional target that exits.
      target_trail  - target never exits; each time it is reached the stop-loss moves
                      to it and the target advances by trailPoints.
    """
    if isinstance(ltp, bool) or not isinstance(ltp, (int, float)) or not math.isfinite(ltp) or ltp <= 0:
        return False, None

    sign = _sign(rule)
    sl = rule["slPrice"]
    if sl is not None and sign * (ltp - sl) <= 0:
        return False, "Stop-loss"

    changed = False
    mode = rule["mode"]
    target = rule.get("targetPrice")
    if mode == "trailing_sl" and sl is not None:
        candidate = round(ltp - sign * rule["slPoints"], 2)
        if sign * (candidate - sl) > 0:
            rule["slPrice"] = candidate
            changed = True
    elif mode == "target_trail":
        next_sl = sl
        while target is not None and sign * (ltp - target) >= 0:
            next_target = round(target + sign * rule["trailPoints"], 2)
            if (not math.isfinite(next_target) or next_target <= 0 or
                    sign * (next_target - target) <= 0):
                raise ValueError("Invalid next target price")
            next_sl = target
            target = next_target
            changed = True
        if changed:
            rule["slPrice"] = next_sl
            rule["targetPrice"] = target
        return changed, None

    if target is not None and sign * (ltp - target) >= 0:
        return changed, "Target"
    return changed, None


class RiskMonitor:
    def __init__(self):
        self.rules = {}

    @property
    def instruments(self):
        return {rule["instrument"] for rule in self.rules.values()}

    def sync(self, remote_rules):
        """Merges the API's rules. A rule edited by the user (new rev) replaces local state."""
        merged = {}
        for remote in remote_rules:
            local = self.rules.get(remote["tsym"])
            if local is not None and local["rev"] == remote["rev"]:
                merged[remote["tsym"]] = local
                continue
            rule = dict(remote)
            rule["token"] = remote["instrument"].split("|", 1)[1]
            rule["dirty"] = False
            rule["triggered"] = False
            merged[remote["tsym"]] = rule
        self.rules = merged

    def on_quote(self, token, ltp):
        """Returns triggers as (rule, reason, ltp); a rule triggers at most once."""
        triggers = []
        for rule in self.rules.values():
            if rule["token"] != token or rule["triggered"]:
                continue
            try:
                changed, reason = evaluate(rule, ltp)
            except ValueError as error:
                if not rule.get("monitor_error_reported"):
                    rule["monitor_error"] = str(error)
                    rule["monitor_error_reported"] = True
                continue
            if reason:
                rule["triggered"] = True
                triggers.append((rule, reason, ltp))
            elif changed:
                rule["dirty"] = True
        return triggers

    def take_dirty(self):
        dirty = [rule for rule in self.rules.values() if rule["dirty"] and not rule["triggered"]]
        for rule in dirty:
            rule["dirty"] = False
        return dirty
