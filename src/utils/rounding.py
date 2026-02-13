from __future__ import annotations

import math


def round_price_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        raise ValueError("tick_size must be > 0")
    return round(math.floor(price / tick_size) * tick_size, 8)


def round_size_to_step(size: float, step_size: float) -> float:
    if step_size <= 0:
        raise ValueError("step_size must be > 0")
    return round(math.floor(size / step_size) * step_size, 8)
