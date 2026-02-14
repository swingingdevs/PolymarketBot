import pytest

from config import Settings


def test_profile_defaults_are_applied() -> None:
    settings = Settings(settings_profile="live")
    assert settings.watch_return_threshold == 0.006
    assert settings.hammer_secs == 12
    assert settings.d_min == 6.0
    assert settings.max_entry_price == 0.93
    assert settings.fee_bps == 10.0


def test_profile_can_be_overridden_by_init_values() -> None:
    settings = Settings(settings_profile="paper", fee_bps=15.0, hammer_secs=30)
    assert settings.fee_bps == 15.0
    assert settings.hammer_secs == 30


def test_profile_can_be_parsed_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SETTINGS_PROFILE", "low_vol")
    settings = Settings()
    assert settings.settings_profile == "low_vol"
    assert settings.hammer_secs == 25


def test_explicit_env_overrides_still_win(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SETTINGS_PROFILE", "high_vol")
    monkeypatch.setenv("FEE_BPS", "7.5")
    settings = Settings()
    assert settings.settings_profile == "high_vol"
    assert settings.fee_bps == 7.5


def test_invalid_profile_is_rejected_with_useful_error() -> None:
    with pytest.raises(ValueError, match=r"Unknown settings_profile=bad_profile.*Allowed"):
        Settings(settings_profile="bad_profile")


def test_unsafe_configuration_rejected() -> None:
    with pytest.raises(ValueError):
        Settings(max_entry_price=0.995)
    with pytest.raises(ValueError):
        Settings(fee_bps=0)
