import pytest

from config import Settings


def test_profile_defaults_are_applied() -> None:
    settings = Settings(settings_profile="live")
    assert settings.watch_return_threshold == 0.006
    assert settings.hammer_secs == 12
    assert settings.max_entry_price == 0.93


def test_profile_can_be_overridden() -> None:
    settings = Settings(settings_profile="paper", fee_bps=15.0)
    assert settings.fee_bps == 15.0


def test_unsafe_configuration_rejected() -> None:
    with pytest.raises(ValueError):
        Settings(max_entry_price=0.995)
    with pytest.raises(ValueError):
        Settings(fee_bps=-0.1)


def test_zero_fee_bps_is_allowed() -> None:
    settings = Settings(fee_bps=0)
    assert settings.fee_bps == 0
