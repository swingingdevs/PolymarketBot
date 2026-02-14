from __future__ import annotations

from typing import Literal

TradeSide = Literal["BUY", "SELL"]


def polymarket_fee_quote(
    *,
    base_rate_bps: float,
    price: float,
    size: float,
) -> float:
    """Fee charged in quote collateral for sells.

    Formula from Polymarket docs for fee-enabled markets:
    feeQuote = baseRate * min(price, 1 - price) * size
    where baseRate is in decimal form.
    """
    if base_rate_bps < 0:
        raise ValueError("base_rate_bps must be >= 0")
    if not (0 < price < 1):
        raise ValueError("price must be between 0 and 1")
    if size <= 0:
        raise ValueError("size must be > 0")

    base_rate = base_rate_bps / 10_000.0
    return base_rate * min(price, 1 - price) * size


def polymarket_fee_base(
    *,
    base_rate_bps: float,
    price: float,
    size: float,
) -> float:
    """Fee charged in base outcome tokens for buys.

    Formula from Polymarket docs for fee-enabled markets:
    feeBase = baseRate * min(price, 1 - price) * (size / price)
    """
    if base_rate_bps < 0:
        raise ValueError("base_rate_bps must be >= 0")
    if not (0 < price < 1):
        raise ValueError("price must be between 0 and 1")
    if size <= 0:
        raise ValueError("size must be > 0")

    base_rate = base_rate_bps / 10_000.0
    return base_rate * min(price, 1 - price) * (size / price)


def fee_cost_per_share_quote_equivalent(*, base_rate_bps: float, price: float, side: TradeSide) -> float:
    """Return fee cost per 1 share in quote-equivalent terms.

    This normalization allows EV calculations to subtract fee as a scalar in
    the same units as price. The BUY/SELL distinction is retained for clarity
    even though quote-equivalent cost is symmetric.
    """
    if side == "SELL":
        return polymarket_fee_quote(base_rate_bps=base_rate_bps, price=price, size=1.0)
    if side == "BUY":
        fee_in_base = polymarket_fee_base(base_rate_bps=base_rate_bps, price=price, size=1.0)
        return fee_in_base * price
    raise ValueError(f"unsupported side: {side}")
