import json
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import pytz
from pykrx import stock


KST = pytz.timezone("Asia/Seoul")

PUBLIC_DIR = Path("docs")
HISTORY_DIR = PUBLIC_DIR / "history"


INVESTORS = {
    "pension": "연기금",
    "foreigner": "외국인",
    "institution": "기관합계",
    "individual": "개인",
}

MARKETS = ["KOSPI", "KOSDAQ"]


def now_kst() -> datetime:
    return datetime.now(KST)


def now_iso() -> str:
    return now_kst().isoformat(timespec="seconds")


def to_yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def latest_business_day() -> str:
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


def df_to_records(df: pd.DataFrame, market: str, investor_key: str, investor_name: str, limit: int = 20):
    records = []

    if df is None or df.empty:
        return records

    df = df.copy()

    if "순매수거래대금" in df.columns:
        df = df.sort_values("순매수거래대금", ascending=False)

    for rank, (code, row) in enumerate(df.head(limit).iterrows(), start=1):
        stock_name = row.get("종목명", "")

        records.append({
            "rank": rank,
            "market": market,
            "investor_key": investor_key,
            "investor": investor_name,
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


def fetch_netbuy_top20(date: str, market: str, investor_key: str, investor_name: str):
    df = stock.get_market_net_purchases_of_equities(
        date,
        date,
        market,
        investor_name
    )
    return df_to_records(
        df,
        market=market,
        investor_key=investor_key,
        investor_name=investor_name,
        limit=20
    )


def fetch_with_retry(market: str, investor_key: str, investor_name: str, max_retry: int = 10):
    base = datetime.strptime(latest_business_day(), "%Y%m%d")

    for i in range(max_retry):
        target = base - timedelta(days=i)
        date = to_yyyymmdd(target)

        try:
            records = fetch_netbuy_top20(date, market, investor_key, investor_name)
            if records:
                return date, records
        except Exception:
            pass

    return None, []


def main():
    PUBLIC_DIR.mkdir(exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    warnings = []
    trade_dates = []

    data_by_investor = {
        "pension": {"kospi": [], "kosdaq": []},
        "foreigner": {"kospi": [], "kosdaq": []},
        "institution": {"kospi": [], "kosdaq": []},
        "individual": {"kospi": [], "kosdaq": []},
    }

    all_records = []

    for investor_key, investor_name in INVESTORS.items():
        for market in MARKETS:
            trade_date, records = fetch_with_retry(market, investor_key, investor_name)

            if trade_date:
                trade_dates.append(trade_date)
                all_records.extend(records)

                market_key = market.lower()
                data_by_investor[investor_key][market_key] = records
            else:
                warnings.append(f"{market} {investor_name} 순매수 데이터를 가져오지 못했습니다.")

    payload = {
        "success": True,
        "app_name": "KRX 수급 노트",
        "trade_date": max(trade_dates) if trade_dates else None,
        "generated_at": now_iso(),
        "source": "KRX / pykrx",
        "ranking_basis": "NET_BUY_VALUE_TOP20",
        "markets": MARKETS,
        "investors": INVESTORS,

        "pension": data_by_investor["pension"],
        "foreigner": data_by_investor["foreigner"],
        "institution": data_by_investor["institution"],
        "individual": data_by_investor["individual"],

        "all_records": all_records,

        "warnings": warnings,
    }

    latest_path = PUBLIC_DIR / "latest.json"
    history_name = f"{payload['trade_date'] or 'unknown'}-investor-netbuy.json"
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

    for investor_key, investor_name in INVESTORS.items():
        kospi_count = len(data_by_investor[investor_key]["kospi"])
        kosdaq_count = len(data_by_investor[investor_key]["kosdaq"])
        print(f"[INFO] {investor_name}: KOSPI={kospi_count}, KOSDAQ={kosdaq_count}")


if __name__ == "__main__":
    main()
