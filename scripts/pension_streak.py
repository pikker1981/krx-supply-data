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

    # 현재 JSON 표준 구조
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

    for idx, trade_date in enumerate(recent_dates):
        if idx == 0:
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

    latest_candidates = {}

    for row in latest_rows:
        code = str(row.get("ISU_SRT_CD", ""))
        if not code:
            continue

        latest_candidates[code] = {
            **row,
            "streak_days": 1,
            "latest_rank": row.get("rank"),
        }

    # 최신일 다음 날짜부터 과거로 연속성 확인
    for day_index in range(1, len(day_payloads)):
        _, payload = day_payloads[day_index]
        rows = get_pension_rows(payload, market_key)
        codes_today = {str(r.get("ISU_SRT_CD", "")) for r in rows if r.get("ISU_SRT_CD")}

        for code, item in list(latest_candidates.items()):
            # 현재까지 day_index일 만큼 연속된 종목만 다음 일자 검사 대상
            if item["streak_days"] == day_index and code in codes_today:
                item["streak_days"] = day_index + 1

    result = [
        item for item in latest_candidates.values()
        if item.get("streak_days", 0) >= min_streak
    ]

    result.sort(
        key=lambda x: (
            x.get("streak_days", 0),
            x.get("NETBID_TRDVAL", 0),
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
