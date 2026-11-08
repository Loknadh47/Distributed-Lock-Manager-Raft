from dlm.lock_fsm import LockStateMachine


def cmd(op, ts, resource="r", owner="a", **kw):
    return {"op": op, "ts": ts, "resource": resource, "owner": owner, **kw}


def test_acquire_release_cycle_and_fencing_tokens_increase():
    fsm = LockStateMachine()
    r1 = fsm.apply(cmd("acquire", 1000, ttl_ms=500))
    assert r1["ok"] and r1["token"] == 1
    assert fsm.apply(cmd("release", 1100, token=1))["ok"]
    r2 = fsm.apply(cmd("acquire", 1200, owner="b", ttl_ms=500))
    assert r2["ok"] and r2["token"] == 2  # token never goes backwards, even after release


def test_mutual_exclusion_and_holder_reported():
    fsm = LockStateMachine()
    fsm.apply(cmd("acquire", 1000, ttl_ms=500))
    r = fsm.apply(cmd("acquire", 1100, owner="b", ttl_ms=500))
    assert not r["ok"] and r["code"] == "HELD" and r["holder"] == "a"


def test_lease_expiry_allows_takeover_with_new_token():
    fsm = LockStateMachine()
    fsm.apply(cmd("acquire", 1000, ttl_ms=100))
    r = fsm.apply(cmd("acquire", 1100, owner="b", ttl_ms=100))  # expires_at == now -> expired
    assert r["ok"] and r["holder"] == "b" and r["token"] == 2


def test_stale_holder_cannot_release_or_renew_after_takeover():
    fsm = LockStateMachine()
    fsm.apply(cmd("acquire", 1000, ttl_ms=100))
    fsm.apply(cmd("acquire", 2000, owner="b", ttl_ms=1000))
    assert fsm.apply(cmd("release", 2001, token=1))["code"] == "NOT_HOLDER"
    assert fsm.apply(cmd("renew", 2001, token=1, ttl_ms=100))["code"] == "NOT_HOLDER"
    assert fsm.query("r")["holder"] in ("b", "")  # b still owns it (or lease elapsed in wall time)


def test_expired_lease_cannot_be_renewed_or_released():
    fsm = LockStateMachine()
    fsm.apply(cmd("acquire", 1000, ttl_ms=100))
    assert fsm.apply(cmd("renew", 1200, token=1, ttl_ms=100))["code"] == "EXPIRED"
    assert fsm.apply(cmd("release", 1200, token=1))["code"] == "EXPIRED"


def test_renew_extends_lease():
    fsm = LockStateMachine()
    fsm.apply(cmd("acquire", 1000, ttl_ms=100))
    r = fsm.apply(cmd("renew", 1050, token=1, ttl_ms=100))
    assert r["ok"] and r["expires_at_ms"] == 1150
    assert not fsm.apply(cmd("acquire", 1140, owner="b", ttl_ms=100))["ok"]


def test_same_owner_reacquire_is_idempotent_and_keeps_token():
    fsm = LockStateMachine()
    first = fsm.apply(cmd("acquire", 1000, ttl_ms=100))
    again = fsm.apply(cmd("acquire", 1050, ttl_ms=100))
    assert again["ok"] and again["token"] == first["token"] and again["expires_at_ms"] == 1150


def test_clock_is_monotonic_even_if_a_command_carries_an_older_timestamp():
    fsm = LockStateMachine()
    fsm.apply(cmd("acquire", 5000, ttl_ms=100))
    # a new leader with a slower clock stamps ts=1000; FSM time must not go backwards
    r = fsm.apply(cmd("acquire", 1000, owner="b", ttl_ms=100))
    assert not r["ok"] and r["code"] == "HELD"


def test_resources_are_independent():
    fsm = LockStateMachine()
    assert fsm.apply(cmd("acquire", 1000, resource="x", ttl_ms=100))["ok"]
    assert fsm.apply(cmd("acquire", 1000, resource="y", owner="b", ttl_ms=100))["ok"]


def test_replay_is_deterministic():
    log = [
        cmd("acquire", 1000, ttl_ms=100),
        cmd("acquire", 1010, owner="b", ttl_ms=100),
        cmd("renew", 1050, token=1, ttl_ms=100),
        cmd("release", 1060, token=1),
        cmd("acquire", 1070, owner="b", ttl_ms=100),
    ]
    a, b = LockStateMachine(), LockStateMachine()
    out_a = [a.apply(dict(c)) for c in log]
    out_b = [b.apply(dict(c)) for c in log]
    assert out_a == out_b and a.snapshot() == b.snapshot()
