from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Set

from pykrx import stock


MARKETS = {
    "kospi": "KOSPI",
    "kosdaq": "KOSDAQ",
}

INVESTOR_NAME = "연기금"


def to_int(value, default=0) -> int:
    try:
        if value is None:
            return default

        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if value == "":
                return default

        return int(float(value))

    except Exception:
        return default


def get_name(ticker: str, fallback: str = "") -> str:
    if fallback:
        return fallback

    try:
        return stock.get_market_ticker_name(ticker)
    except Exception:
        return ""


def fetch_pension_positive_rows(trade_date: str, market_key: str) -> Dict[str, dict]:
    """
    특정 거래일/시장 기준 연기금 순매수 종목 전체를 조회한다.

    중요:
    - TOP20만 보는 게 아니라 전체 종목 중 순매수거래대금 > 0인 종목을 대상으로 한다.
    - 그래서 '7일 연속 순매수'의 의미가 TOP20 연속 진입이 아니라 실제 연속 순매수가 된다.
    """
    market = MARKETS[market_key]

    df = stock.get_market_net_purchases_of_equities(
        trade_date,
        trade_date,
        market,
        INVESTOR_NAME,
    )

    if df is None or df.empty:
        return {}

    df = df.copy()

    net_value_col = "순매수거래대금"

    if net_value_col not in df.columns:
        possible_cols = [
            col for col in df.columns
            if "순매수" in str(col) and "대금" in str(col)
        ]

        if not possible_cols:
            return {}

        net_value_col = possible_cols[0]

    result = {}

    for ticker, row in df.iterrows():
        row_dict = row.to_dict()
        net_value = to_int(row_dict.get(net_value_col))

        if net_value <= 0:
            continue

        ticker = str(ticker)
        name = str(row_dict.get("종목명", "") or get_name(ticker))

        result[ticker] = {
            "trade_date": trade_date,
            "market": market,
            "investor_key": "pension",
            "investor": INVESTOR_NAME,
            "ISU_SRT_CD": ticker,
            "ISU_ABBRV": name,
            "NETBID_TRDVAL": net_value,
            "NETBID_TRDVOL": to_int(row_dict.get("순매수거래량")),
            "ASK_TRDVAL": to_int(row_dict.get("매도거래대금")),
            "BID_TRDVAL": to_int(row_dict.get("매수거래대금")),
            "ASK_TRDVOL": to_int(row_dict.get("매도거래량")),
            "BID_TRDVOL": to_int(row_dict.get("매수거래량")),
        }

    return result


def compute_market_streak_from_krx(
    recent_trade_dates: List[str],
    market_key: str,
    min_streak: int,
    lookback: int,
) -> List[dict]:
    """
    최근 거래일 목록을 기준으로 최신일부터 과거 방향으로 연속 순매수 여부를 계산한다.

    계산 방식:
    1. 최신 거래일에 연기금 순매수인 전체 종목을 후보로 잡음
    2. 바로 전 거래일에도 순매수였는지 확인
    3. 끊기면 중단
    4. min_streak 이상이면 결과 포함
    """
    dates = (recent_trade_dates or [])[:lookback]

    if len(dates) < min_streak:
        return []

    daily_rows: List[Dict[str, dict]] = []

    for trade_date in dates:
        try:
            rows = fetch_pension_positive_rows(trade_date, market_key)
            daily_rows.append(rows)
            print(f"[pension_streak/{market_key}] {trade_date}: {len(rows)} positive rows")

        except Exception as exc:
            print(f"[pension_streak/{market_key}] {trade_date} 조회 실패: {exc}")
            daily_rows.append({})

    if not daily_rows or not daily_rows[0]:
        return []

    latest_rows = daily_rows[0]
    candidates: Set[str] = set(latest_rows.keys())

    result = []

    for code in candidates:
        streak_days = 0

        for day_map in daily_rows:
            if code in day_map:
                streak_days += 1
            else:
                break

        if streak_days >= min_streak:
            latest_item = dict(latest_rows[code])
            latest_item["streak_days"] = streak_days
            latest_item["latest_rank"] = None
            result.append(latest_item)

    result.sort(
        key=lambda item: (
            item.get("streak_days", 0),
            item.get("NETBID_TRDVAL", 0),
        ),
        reverse=True,
    )

    for idx, item in enumerate(result, start=1):
        item["rank"] = idx

    return result


def compute_pension_streak(
    latest_payload: dict,
    history_dir: Path | None = None,
    min_streak: int = 7,
    lookback: int = 30,
) -> dict:
    """
    collect.py에서 호출하는 진입점.

    history_dir 인자는 기존 호출 호환용으로 받지만,
    실제 계산은 KRX/pykrx 전체 연기금 순매수 데이터 기준으로 수행한다.
    """
    recent_trade_dates = latest_payload.get("recent_trade_dates") or []

    return {
        "kospi": compute_market_streak_from_krx(
            recent_trade_dates=recent_trade_dates,
            market_key="kospi",
            min_streak=min_streak,
            lookback=lookback,
        ),
        "kosdaq": compute_market_streak_from_krx(
            recent_trade_dates=recent_trade_dates,
            market_key="kosdaq",
            min_streak=min_streak,
            lookback=lookback,
        ),
    }
