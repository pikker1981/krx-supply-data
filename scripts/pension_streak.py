"""
연기금 연속 순매수 종목 계산 모듈.

오늘(latest_trade_date) 기준 과거 N영업일을 거슬러 올라가며
NETBID_TRDVAL > 0 인 일수가 연속으로 이어지는 종목을 시장(KOSPI/KOSDAQ)별
로 추출한다.

출력 스키마(scripts/collect.py에서 latest.json["pension_streak"]에 그대로
주입):
    [
      {
        "market": "KOSPI" | "KOSDAQ",
        "ISU_SRT_CD": "005930",
        "ISU_ABBRV": "삼성전자",
        "streak_days": 12,
        "NETBID_TRDVAL": 12_043_040_750,
        "NETBID_TRDVOL": 56_139,
        "first_streak_date": "20260415",
        "latest_trade_date": "20260429"
      },
      ...
    ]

호출 비용 절감을 위해 일자×시장 단위로 parquet 캐시를 둔다.
GitHub Actions 환경에서는 캐시가 매 실행마다 초기화될 수 있으므로
cache/ 디렉토리를 워크플로우에서 actions/cache 등으로 보존하면 더욱 빨라진다.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd
from pykrx import stock


INVESTOR_NAME = "연기금"
DEFAULT_MIN_STREAK = 7
DEFAULT_LOOKBACK = 30
MARKETS = ("KOSPI", "KOSDAQ")

CACHE_DIR = Path("cache/pension_netbuy")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _previous_business_dates(latest_yyyymmdd: str, count: int) -> List[str]:
    """latest 포함 과거 N영업일을 최신→과거 순으로 반환."""
    dates: List[str] = [latest_yyyymmdd]
    cursor = datetime.strptime(latest_yyyymmdd, "%Y%m%d")
    safety = 0
    while len(dates) < count and safety < count * 3:
        cursor -= timedelta(days=1)
        safety += 1
        d = cursor.strftime("%Y%m%d")
        try:
            df = stock.get_market_ohlcv(d, market="KOSPI")
        except Exception:
            continue
        if df is not None and not df.empty:
            dates.append(d)
    return dates


def _fetch_netbuy_one_day(date: str, market: str) -> pd.DataFrame:
    """해당 일자·시장의 연기금 종목별 순매수 데이터를 pykrx로 조회."""
    df = stock.get_market_net_purchases_of_equities(
        date, date, market, INVESTOR_NAME
    )
    if df is None:
        return pd.DataFrame()
    return df


def _fetch_netbuy_cached(date: str, market: str) -> pd.DataFrame:
    """parquet 캐시를 활용한 1일치 net-buy 조회."""
    cache_path = CACHE_DIR / f"{date}-{market}.parquet"
    if cache_path.exists():
        try:
            return pd.read_parquet(cache_path)
        except Exception:
            cache_path.unlink(missing_ok=True)

    df = _fetch_netbuy_one_day(date, market)
    if df is not None and not df.empty:
        try:
            df.to_parquet(cache_path)
        except Exception:
            pass
    return df


def compute_pension_streak(
    latest_yyyymmdd: str,
    min_streak: int = DEFAULT_MIN_STREAK,
    lookback: int = DEFAULT_LOOKBACK,
) -> List[Dict]:
    """연기금 연속 순매수 streak 산출. 상위로 정렬해 반환."""
    business_dates = _previous_business_dates(latest_yyyymmdd, lookback)
    results: List[Dict] = []

    for market in MARKETS:
        # 일자별 DataFrame 캐시
        per_day: Dict[str, pd.DataFrame] = {}
        for d in business_dates:
            df = _fetch_netbuy_cached(d, market)
            if df is None or df.empty:
                continue
            per_day[d] = df

        if not per_day:
            continue

        sorted_dates = sorted(per_day.keys(), reverse=True)  # 최신 → 과거
        latest_d = sorted_dates[0]
        latest_df = per_day[latest_d]

        # 시작 후보: 최신일에 순매수 > 0 인 모든 종목
        if "순매수거래대금" not in latest_df.columns:
            continue
        start_codes = latest_df.index[latest_df["순매수거래대금"] > 0].tolist()

        for code in start_codes:
            streak = 1
            first_date = latest_d
            for d in sorted_dates[1:]:
                df_d = per_day[d]
                if code not in df_d.index:
                    break
                val = df_d.at[code, "순매수거래대금"]
                if pd.isna(val) or val <= 0:
                    break
                streak += 1
                first_date = d

            if streak >= min_streak:
                latest_row = latest_df.loc[code]
                results.append({
                    "market": market,
                    "ISU_SRT_CD": str(code),
                    "ISU_ABBRV": str(latest_row.get("종목명", "")),
                    "streak_days": int(streak),
                    "NETBID_TRDVAL": int(latest_row.get("순매수거래대금", 0) or 0),
                    "NETBID_TRDVOL": int(latest_row.get("순매수거래량", 0) or 0),
                    "first_streak_date": first_date,
                    "latest_trade_date": latest_d,
                })

    # streak 내림차순 → 순매수 거래대금 내림차순
    results.sort(key=lambda r: (-r["streak_days"], -r["NETBID_TRDVAL"]))
    return results


if __name__ == "__main__":
    # 단독 실행 디버그
    import json
    import sys

    target = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y%m%d")
    out = compute_pension_streak(target, min_streak=7, lookback=30)
    print(f"[pension_streak] target={target} count={len(out)}")
    print(json.dumps(out[:10], ensure_ascii=False, indent=2))
