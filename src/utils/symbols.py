from __future__ import annotations


_KNOWN_QUOTES: tuple[str, ...] = ("usdt", "usdc", "usd", "eur", "gbp")


def normalize_symbol(symbol: str) -> str:
    """Normalize symbol text to `base/quote` lowercase form."""
    value = symbol.strip().lower()
    if not value:
        raise ValueError("symbol must not be empty")

    compact = "".join(char for char in value if char.isalnum() or char in {"/", "-"})
    if not compact:
        raise ValueError(f"invalid symbol={symbol!r}")

    if "/" in compact or "-" in compact:
        separator = "/" if "/" in compact else "-"
        parts = [part for part in compact.split(separator) if part]
        if len(parts) != 2:
            raise ValueError(f"invalid symbol={symbol!r}")
        base, quote = parts
    else:
        base = ""
        quote = ""
        for candidate_quote in _KNOWN_QUOTES:
            if compact.endswith(candidate_quote) and len(compact) > len(candidate_quote):
                base = compact[: -len(candidate_quote)]
                quote = candidate_quote
                break
        if not base or not quote:
            raise ValueError(f"invalid symbol={symbol!r}; expected format like BTC/USD or BTCUSD")

    if not base.isalnum() or not quote.isalnum():
        raise ValueError(f"invalid symbol={symbol!r}")

    return f"{base}/{quote}"

