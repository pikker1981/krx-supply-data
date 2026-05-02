"""
investor_streak.py
──────────────────
투자자별 연속 순매수 계산 모듈.

두 가지 방식 제공:
  - compute_streak_from_krx()      : KRX 전체 종목 조회 (연기금 전용 — 비 TOP20 축적 포착)
  - compute_streak_from_history()  : 이미 수집된 history JSON의 TOP20 기반 (외국인/기관 — 빠름)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from pykrx import stock


MARKETS: Dict[str, str] = {
    "kospi": "KOSPI",
    "kosdaq": "KOSDAQ",
}

INVESTOR_NAMES: Dict[str, str] = {
    "pension": "연기금",
    "foreigner": "외국인",
    "institution": "기관합계",
    "individual": "개인",
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _to_int(value, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if not value:
                return default
        return int(float(value))
    except Exception:
        return default


def _count_streak(daily_maps: List[Dict[str, dict]], code: str) -> int:
    """최신일(index 0)부터 연속으로 code 가 포함된 날 수를 반환."""
    s = 0
    for day_map in daily_maps:
        if code in day_map:
            s += 1
        else:
            break
    return s


def _sort_and_rank(items: List[dict]) -> List[dict]:
    items.sort(
        key=lambda x: (x.get("streak_days", 0), x.get("NETBID_TRDVAL", 0)),
        reverse=True,
    )
    for idx, item in enumerate(items, start=1):
        item["rank"] = idx
    return items


# ── KRX full-data approach (pension) ──────────────────────────────────────────

def _fetch_positive_rows_krx(
    trade_date: str,
    market_key: str,
    investor_key: str,
) -> Dict[str, dict]:
    """해당 거래일의 투자자 순매수 전체 종목 조회 (순매수거래대금 > 0)."""
    market = MARKETS[market_key]
    investor_name = INVESTOR_NAMES[investor_key]

    df = stock.get_market_net_purchases_of_equities(
        trade_date, trade_date, market, investor_name,
    )

    if df is None or df.empty:
        return {}

    df = df.copy()
    net_col = "순매수거래대금"

    if net_col not in df.columns:
        candidates = [c for c in df.columns if "순매수" in str(c) and "대금" in str(c)]
        if not candidates:
            return {}
        net_col = candidates[0]

    result: Dict[str, dict] = {}

    for ticker, row in df.iterrows():
        d = row.to_dict()
        net_value = _to_int(d.get(net_col))
        if net_value <= 0:
            continue
        ticker = str(ticker)
        result[ticker] = {
            "trade_date": trade_date,
            "market": market,
            "investor_key": investor_key,
            "investor": investor_name,
            "ISU_SRT_CD": ticker,
            "ISU_ABBRV": str(d.get("종목명", "")),
            "NETBID_TRDVAL": net_value,
            "NETBID_TRDVOL": _to_int(d.get("순매수거래량")),
            "ASK_TRDVAL": _to_int(d.get("매도거래대금")),
            "BID_TRDVAL": _to_int(d.get("매수거래대금")),
            "ASK_TRDVOL": _to_int(d.get("매도거래량")),
            "BID_TRDVOL": _to_int(d.get("매수거래량")),
        }

    return result


def compute_streak_from_krx(
    latest_payload: dict,
    investor_key: str,
    min_streak: int = 7,
    lookback: int = 30,
) -> dict:
    """
    KRX API 전체 종목 조회 기반 연속 순매수 계산.
    연기금처럼 TOP20 진입 전의 조용한 축적을 포착할 때 사용.
    """
    recent_dates = (latest_payload.get("recent_trade_dates") or [])[:lookback]
    result: Dict[str, list] = {}

    for market_key in ("kospi", "kosdaq"):
        if len(recent_dates) < min_streak:
            result[market_key] = []
            continue

        daily: List[Dict[str, dict]] = []

        for date in recent_dates:
            try:
                rows = _fetch_positive_rows_krx(date, market_key, investor_key)
                daily.append(rows)
                print(f"[krx_streak/{investor_key}/{market_key}] {date}: {len(rows)}")
            except Exception as exc:
                print(f"[krx_streak/{investor_key}/{market_key}] {date} 실패: {exc}")
                daily.append({})

        if not daily or not daily[0]:
            result[market_key] = []
            continue

        streaks = []
        for code, latest_row in daily[0].items():
            s = _count_streak(daily, code)
            if s >= min_streak:
                item = dict(latest_row)
                item["streak_days"] = s
                item["latest_rank"] = None
                streaks.append(item)

        result[market_key] = _sort_and_rank(streaks)

    return result


# ── History-file approach (foreigner / institution) ───────────────────────────

def _load_history(trade_date: str, history_dir: Path, latest_payload: dict) -> dict | None:
    if trade_date == latest_payload.get("trade_date"):
        return latest_payload
    path = history_dir / f"{trade_date}-investor-netbuy.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def compute_streak_from_history(
    latest_payload: dict,
    history_dir: Path,
    investor_key: str,
    min_streak: int = 5,
    lookback: int = 30,
) -> dict:
    """
    수집된 history JSON의 TOP20 데이터 기반 연속 순매수 계산.
    추가 API 호출 없이 빠르게 동작. 외국인/기관합계에 적합.
    """
    recent_dates = (latest_payload.get("recent_trade_dates") or [])[:lookback]
    result: Dict[str, list] = {}

    for market_key in ("kospi", "kosdaq"):
        if len(recent_dates) < min_streak:
            result[market_key] = []
            continue

        daily_maps: List[Dict[str, dict]] = []

        for date in recent_dates:
            payload = _load_history(date, history_dir, latest_payload)
            if payload is None:
                daily_maps.append({})
                continue
            rows = payload.get(investor_key, {}).get(market_key, [])
            code_map = {
                str(r.get("ISU_SRT_CD")): r
                for r in rows
                if r.get("ISU_SRT_CD")
            }
            daily_maps.append(code_map)
            print(f"[hist_streak/{investor_key}/{market_key}] {date}: {len(code_map)}")

        if not daily_maps or not daily_maps[0]:
            result[market_key] = []
            continue

        streaks = []
        for code, latest_row in daily_maps[0].items():
            s = _count_streak(daily_maps, code)
            if s >= min_streak:
                item = dict(latest_row)
                item["streak_days"] = s
                streaks.append(item)

        result[market_key] = _sort_and_rank(streaks)

    return result
