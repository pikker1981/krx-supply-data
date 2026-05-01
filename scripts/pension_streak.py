import json
from pathlib import Path


MARKETS = ["kospi", "kosdaq"]


def load_history(history_dir: Path, trade_date: str) -> dict | None:
    path = history_dir / f"{trade_date}-investor-netbuy.json"

    if not path.exists():
        return None

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def get_pension_rows(payload: dict | None, market_key: str) -> list[dict]:
    if not payload:
        return []

    rows = payload.get("pension", {}).get(market_key, [])

    if isinstance(rows, list):
        return rows

    return []


def compute_market_streak(
    latest_payload: dict,
    history_dir: Path,
    market_key: str,
    min_streak: int,
    lookback: int,
) -> list[dict]:
    recent_dates = (latest_payload.get("recent_trade_dates") or [])[:lookback]

    if not recent_dates:
        return []

    day_payloads = []

    for index, trade_date in enumerate(recent_dates):
        if index == 0:
            day_payloads.append((trade_date, latest_payload))
        else:
            payload = load_history(history_dir, trade_date)
            if payload:
                day_payloads.append((trade_date, payload))

    if len(day_payloads) < min_streak:
        return []

    latest_rows = get_pension_rows(latest_payload, market_key)

    if not latest_rows:
        return []

    candidates = {}

    for row in latest_rows:
        code = str(row.get("ISU_SRT_CD", ""))

        if not code:
            continue

        candidates[code] = {
            **row,
            "streak_days": 1,
            "latest_rank": row.get("rank"),
        }

    for day_index in range(1, len(day_payloads)):
        _, payload = day_payloads[day_index]
        rows = get_pension_rows(payload, market_key)
        codes_today = {
            str(row.get("ISU_SRT_CD", ""))
            for row in rows
            if row.get("ISU_SRT_CD")
        }

        for code, item in list(candidates.items()):
            if item["streak_days"] == day_index and code in codes_today:
                item["streak_days"] = day_index + 1

    result = [
        item for item in candidates.values()
        if item.get("streak_days", 0) >= min_streak
    ]

    result.sort(
        key=lambda item: (
            item.get("streak_days", 0),
            item.get("NETBID_TRDVAL", 0),
        ),
        reverse=True,
    )

    return result


def compute_pension_streak(
    latest_payload: dict,
    history_dir: Path,
    min_streak: int = 7,
    lookback: int = 30,
) -> dict:
    return {
        "kospi": compute_market_streak(
            latest_payload=latest_payload,
            history_dir=history_dir,
            market_key="kospi",
            min_streak=min_streak,
            lookback=lookback,
        ),
        "kosdaq": compute_market_streak(
            latest_payload=latest_payload,
            history_dir=history_dir,
            market_key="kosdaq",
            min_streak=min_streak,
            lookback=lookback,
        ),
    }
