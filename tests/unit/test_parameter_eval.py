from pathlib import Path

from markets.gamma_cache import UpDownMarket
from strategy.parameter_eval import replay_with_params, sweep_parameter_grid, export_report, ReplayRow


def _sample_rows() -> list[ReplayRow]:
    t0 = 1_710_000_000
    rows: list[ReplayRow] = []
    for i in range(75):
        rows.append(ReplayRow(ts=t0 + i, event="price", price=50_000 + i * 2.0))
    rows.append(ReplayRow(ts=t0 + 70, event="book", token_id="u5", ask=0.2, bid=0.18))
    rows.append(ReplayRow(ts=t0 + 70, event="book", token_id="d5", ask=0.8, bid=0.75))
    rows.append(ReplayRow(ts=t0 + 70, event="book", token_id="u15", ask=0.25, bid=0.2))
    rows.append(ReplayRow(ts=t0 + 70, event="book", token_id="d15", ask=0.7, bid=0.65))
    return sorted(rows, key=lambda r: r.ts)


def test_replay_with_params_produces_metrics() -> None:
    rows = _sample_rows()
    t0 = int(rows[0].ts)
    markets = [
        UpDownMarket("m5", t0 - 285, t0 + 74, "u5", "d5", 5),
        UpDownMarket("m15", t0 - 885, t0 + 74, "u15", "d15", 15),
    ]
    params = {
        "watch_return_threshold": 0.001,
        "hammer_secs": 100,
        "d_min": 1.0,
        "max_entry_price": 0.99,
        "fee_bps": 10.0,
    }

    out = replay_with_params(rows, markets, params)
    assert int(out["trades"]) >= 1
    assert float(out["trade_frequency_per_hour"]) > 0


def test_sweep_and_export_reports(tmp_path: Path) -> None:
    rows = _sample_rows()
    t0 = int(rows[0].ts)
    markets = [
        UpDownMarket("m5", t0 - 285, t0 + 74, "u5", "d5", 5),
        UpDownMarket("m15", t0 - 885, t0 + 74, "u15", "d15", 15),
    ]
    grid = {
        "watch_return_threshold": [0.001, 0.002],
        "hammer_secs": [80, 100],
        "d_min": [1.0],
        "max_entry_price": [0.95, 0.99],
        "fee_bps": [8.0],
    }

    report, robust_ranges = sweep_parameter_grid(rows, markets, grid)
    assert len(report) == 8
    assert "watch_return_threshold" in robust_ranges

    output_prefix = tmp_path / "calibration" / "run"
    export_report(report, robust_ranges, output_prefix)

    assert output_prefix.with_suffix(".json").exists()
    assert output_prefix.with_suffix(".csv").exists()
