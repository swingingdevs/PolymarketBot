import pytest

from config import Settings


def _clear_profile_tunable_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    tunable_env_vars = [
        "WATCH_RETURN_THRESHOLD",
        "HAMMER_SECS",
        "D_MIN",
        "MAX_ENTRY_PRICE",
        "FEE_BPS",
    ]

    for env_var in tunable_env_vars:
        monkeypatch.delenv(env_var, raising=False)
        monkeypatch.delenv(env_var.lower(), raising=False)


def test_default_profile_is_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_profile_tunable_env_vars(monkeypatch)
    settings = Settings(_env_file=None)
    assert settings.settings_profile == "paper"
    assert settings.watch_return_threshold == 0.004
    assert settings.hammer_secs == 20


def test_profile_defaults_are_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_profile_tunable_env_vars(monkeypatch)
    settings = Settings(settings_profile="live", _env_file=None)
    assert settings.watch_return_threshold == 0.006
    assert settings.hammer_secs == 12
    assert settings.d_min == 6.0
    assert settings.max_entry_price == 0.93
    assert settings.fee_bps == 10.0


def test_explicit_field_overrides_are_not_replaced_by_profile_defaults() -> None:
    settings = Settings(settings_profile="paper", fee_bps=15.0, hammer_secs=30)
    assert settings.fee_bps == 15.0
    assert settings.hammer_secs == 30


def test_profile_can_be_parsed_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_profile_tunable_env_vars(monkeypatch)
    monkeypatch.setenv("SETTINGS_PROFILE", "low_vol")
    settings = Settings(_env_file=None)
    assert settings.settings_profile == "low_vol"
    assert settings.hammer_secs == 25


def test_explicit_env_overrides_still_win(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SETTINGS_PROFILE", "high_vol")
    monkeypatch.setenv("FEE_BPS", "7.5")
    settings = Settings(_env_file=None)
    assert settings.settings_profile == "high_vol"
    assert settings.fee_bps == 7.5


def test_explicit_env_override_matching_baseline_default_still_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SETTINGS_PROFILE", "live")
    monkeypatch.setenv("HAMMER_SECS", "15")
    settings = Settings(_env_file=None)
    assert settings.settings_profile == "live"
    assert settings.hammer_secs == 15


def test_invalid_profile_is_rejected_with_useful_error() -> None:
    with pytest.raises(ValueError, match=r"Input should be 'paper', 'live', 'high_vol' or 'low_vol'"):
        Settings(settings_profile="bad_profile")


def test_unsafe_configuration_rejected() -> None:
    with pytest.raises(ValueError):
        Settings(max_entry_price=0.995)
    with pytest.raises(ValueError):
        Settings(fee_bps=-0.1)


def test_zero_fee_bps_is_allowed() -> None:
    settings = Settings(fee_bps=0)
    assert settings.fee_bps == 0


def test_price_stale_after_seconds_can_be_configured_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRICE_STALE_AFTER_SECONDS", "9.25")
    settings = Settings()
    assert settings.price_stale_after_seconds == 9.25


def test_spot_quorum_min_sources_must_be_at_least_two() -> None:
    with pytest.raises(ValueError):
        Settings(spot_quorum_min_sources=1)


def test_fee_rate_ttl_seconds_must_be_positive() -> None:
    with pytest.raises(ValueError):
        Settings(fee_rate_ttl_seconds=0)


def test_fee_rate_ttl_seconds_can_be_configured_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FEE_RATE_TTL_SECONDS", "120")
    settings = Settings()
    assert settings.fee_rate_ttl_seconds == 120.0


def test_min_trade_interval_seconds_defaults_to_zero() -> None:
    settings = Settings()
    assert settings.min_trade_interval_seconds == 0


def test_min_trade_interval_seconds_must_be_non_negative() -> None:
    with pytest.raises(ValueError):
        Settings(min_trade_interval_seconds=-1)
