import json
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import pytz
from pykrx import stock


KST = pytz.timezone("Asia/Seoul")

PUBLIC_DIR = Path("docs")
HISTORY_DIR = PUBLIC_DIR / "history"


def now_kst() -> datetime:
    return datetime.now(KST)


def now_iso() -> str:
    return now_kst().isoformat(timespec="seconds")


def to_yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def latest_business_day() -> str:
    """
    단순 기준:
    - 평일이면 전일
    - 월요일이면 직전 금요일
    - 토/일이면 직전 금요일

    KRX 휴장일 전체 캘린더까지 반영한 것은 아니므로,
    휴장일에는 pykrx 조회 실패 시 하루씩 뒤로 밀어 재시도한다.
    """
    today = now_kst().date()

    if today.weekday() == 0:      # Monday
        target = today - timedelta(days=3)
    elif today.weekday() == 5:    # Saturday
        target = today - timedelta(days=1)
    elif today.weekday() == 6:    # Sunday
        target = today - timedelta(days=2)
    else:
        target = today - timedelta(days=1)

    return target.strftime("%Y%m%d")


def clean_number(value):
    if pd.isna(value):
        return 0
    try:
        return int(value)
    except Exception:
        try:
            return int(str(value).replace(",", ""))
        except Exception:
            return 0


def df_to_records(df: pd.DataFrame, market: str, investor: str, limit: int = 20):
    records = []

    if df is None or df.empty:
        return records

    # pykrx 반환 컬럼 예: 종목명, 매도거래량, 매수거래량, 순매수거래량, 매도거래대금, 매수거래대금, 순매수거래대금
    df = df.copy()

    if "순매수거래대금" in df.columns:
        df = df.sort_values("순매수거래대금", ascending=False)

    for rank, (code, row) in enumerate(df.head(limit).iterrows(), start=1):
        stock_name = row.get("종목명", "")

        records.append({
            "rank": rank,
            "market": market,
            "investor": investor,
            "ISU_SRT_CD": str(code),
            "ISU_ABBRV": str(stock_name),
            "NETBID_TRDVAL": clean_number(row.get("순매수거래대금", 0)),
            "NETBID_TRDVOL": clean_number(row.get("순매수거래량", 0)),
            "ASK_TRDVAL": clean_number(row.get("매도거래대금", 0)),
            "BID_TRDVAL": clean_number(row.get("매수거래대금", 0)),
            "ASK_TRDVOL": clean_number(row.get("매도거래량", 0)),
            "BID_TRDVOL": clean_number(row.get("매수거래량", 0)),
        })

    return records


def fetch_netbuy_top20(date: str, market: str, investor: str):
    df = stock.get_market_net_purchases_of_equities(
        date,
        date,
        market,
        investor
    )
    return df_to_records(df, market=market, investor=investor, limit=20)


def fetch_with_retry(market: str, investor: str, max_retry: int = 10):
    """
    휴장일/데이터 미반영일 대응:
    기준일에서 하루씩 뒤로 가며 데이터가 나오는 날짜를 찾는다.
    """
    base = datetime.strptime(latest_business_day(), "%Y%m%d")
    last_error = None

    for i in range(max_retry):
        target = base - timedelta(days=i)
        date = to_yyyymmdd(target)

        try:
            records = fetch_netbuy_top20(date, market, investor)
            if records:
                return date, records
        except Exception as exc:
            last_error = str(exc)

    return None, []


def main():
    PUBLIC_DIR.mkdir(exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    warnings = []
    investor_ranks = []
    trade_dates = []

    targets = [
        ("KOSPI", "연기금"),
        ("KOSDAQ", "연기금"),
    ]

    for market, investor in targets:
        trade_date, records = fetch_with_retry(market, investor)

        if trade_date:
            trade_dates.append(trade_date)
            investor_ranks.extend(records)
        else:
            warnings.append(f"{market} {investor} 순매수 데이터를 가져오지 못했습니다.")

    kospi = [item for item in investor_ranks if item["market"] == "KOSPI"]
    kosdaq = [item for item in investor_ranks if item["market"] == "KOSDAQ"]

    payload = {
        "success": True,
        "app_name": "KRX 수급 노트",
        "trade_date": max(trade_dates) if trade_dates else None,
        "generated_at": now_iso(),
        "source": "KRX / pykrx",
        "ranking_basis": "PENSION_NET_BUY_VALUE_TOP20",
        "markets": ["KOSPI", "KOSDAQ"],
        "kospi": kospi,
        "kosdaq": kosdaq,
        "pension": investor_ranks,
        "investor_ranks": investor_ranks,
        "combined_netbuy": investor_ranks,
        "pension_streak": [],
        "warnings": warnings,
    }

    latest_path = PUBLIC_DIR / "latest.json"
    history_name = f"{payload['trade_date'] or 'unknown'}-pension-netbuy.json"
    history_path = HISTORY_DIR / history_name

    latest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    history_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("[DONE] docs/latest.json")
    print(f"[DONE] {history_path}")
    print(f"[INFO] trade_date={payload['trade_date']}")
    print(f"[INFO] kospi_count={len(kospi)}")
    print(f"[INFO] kosdaq_count={len(kosdaq)}")


if __name__ == "__main__":
    main()
