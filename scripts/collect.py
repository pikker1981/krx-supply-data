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


def latest_business_day_dt() -> datetime:
    today = now_kst().date()

    if today.weekday() == 0:
        target = today - timedelta(days=3)
    elif today.weekday() == 5:
        target = today - timedelta(days=1)
    elif today.weekday() == 6:
        target = today - timedelta(days=2)
    else:
        target = today - timedelta(days=1)

    return datetime.combine(target, datetime.min.time())


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


def format_date(date: str) -> str:
    if not date:
        return ""
    return f"{date[:4]}-{date[4:6]}-{date[6:]}"


def df_to_records(df: pd.DataFrame, market: str, investor_key: str, investor_name: str, trade_date: str, limit: int = 20):
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
            "trade_date": trade_date,
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
        trade_date=date,
        limit=20
    )


def fetch_with_retry(market: str, investor_key: str, investor_name: str, base_dt: datetime, max_retry: int = 10):
    for i in range(max_retry):
        target = base_dt - timedelta(days=i)
        date = to_yyyymmdd(target)

        try:
            records = fetch_netbuy_top20(date, market, investor_key, investor_name)
            if records:
                return date, records
        except Exception:
            pass

    return None, []


def fetch_recent_available_days(count: int = 4):
    """
    기준일 포함 최근 거래 가능일을 count개 확보한다.
    휴장일은 조회 결과가 비어 있으므로 자동으로 건너뛴다.
    """
    base_dt = latest_business_day_dt()
    dates = []
    cursor = base_dt

    for _ in range(20):
        date = to_yyyymmdd(cursor)

        try:
            sample = stock.get_market_ohlcv_by_ticker(date, market="KOSPI")
            if sample is not None and not sample.empty:
                dates.append(date)
        except Exception:
            pass

        if len(dates) >= count:
            break

        cursor = cursor - timedelta(days=1)

    return dates


def summarize_records(records):
    total_value = sum(item.get("NETBID_TRDVAL", 0) for item in records)
    top = records[0] if records else None

    return {
        "count": len(records),
        "total_netbuy_value": total_value,
        "top_stock_name": top.get("ISU_ABBRV") if top else None,
        "top_stock_code": top.get("ISU_SRT_CD") if top else None,
        "top_netbuy_value": top.get("NETBID_TRDVAL") if top else 0,
    }


def build_summary(data_by_investor):
    summary = {}

    for investor_key, market_data in data_by_investor.items():
        summary[investor_key] = {
            "kospi": summarize_records(market_data.get("kospi", [])),
            "kosdaq": summarize_records(market_data.get("kosdaq", [])),
        }

    return summary


def code_set(records):
    return set(item["ISU_SRT_CD"] for item in records)


def build_new_entries(today_data, yesterday_data):
    result = {}

    for investor_key in INVESTORS.keys():
        result[investor_key] = {}

        for market_key in ["kospi", "kosdaq"]:
            today_records = today_data.get(investor_key, {}).get(market_key, [])
            yesterday_records = yesterday_data.get(investor_key, {}).get(market_key, [])

            yesterday_codes = code_set(yesterday_records)

            new_items = [
                item for item in today_records
                if item["ISU_SRT_CD"] not in yesterday_codes
            ]

            result[investor_key][market_key] = new_items

    return result


def build_three_day_streak(recent_by_date):
    """
    최근 3거래일 모두 TOP20에 들어온 종목.
    """
    result = {}

    dates = list(recent_by_date.keys())[:3]

    if len(dates) < 3:
        return result

    for investor_key in INVESTORS.keys():
        result[investor_key] = {}

        for market_key in ["kospi", "kosdaq"]:
            sets = []

            for date in dates:
                records = recent_by_date[date].get(investor_key, {}).get(market_key, [])
                sets.append(code_set(records))

            common_codes = set.intersection(*sets) if sets else set()
            today_records = recent_by_date[dates[0]].get(investor_key, {}).get(market_key, [])

            streak_items = [
                item for item in today_records
                if item["ISU_SRT_CD"] in common_codes
            ]

            result[investor_key][market_key] = streak_items

    return result


def build_overlap(today_data):
    """
    같은 시장에서 여러 투자자군이 동시에 순매수 TOP20에 올린 종목.
    """
    result = {
        "kospi": [],
        "kosdaq": [],
    }

    for market_key in ["kospi", "kosdaq"]:
        stock_map = {}

        for investor_key, investor_name in INVESTORS.items():
            records = today_data.get(investor_key, {}).get(market_key, [])

            for item in records:
                code = item["ISU_SRT_CD"]

                if code not in stock_map:
                    stock_map[code] = {
                        "ISU_SRT_CD": code,
                        "ISU_ABBRV": item["ISU_ABBRV"],
                        "market": item["market"],
                        "investors": [],
                        "total_netbuy_value": 0,
                    }

                stock_map[code]["investors"].append(investor_name)
                stock_map[code]["total_netbuy_value"] += item.get("NETBID_TRDVAL", 0)

        overlaps = [
            item for item in stock_map.values()
            if len(item["investors"]) >= 2
        ]

        overlaps.sort(key=lambda x: x["total_netbuy_value"], reverse=True)
        result[market_key] = overlaps

    return result


def empty_investor_market_data():
    return {
        "pension": {"kospi": [], "kosdaq": []},
        "foreigner": {"kospi": [], "kosdaq": []},
        "institution": {"kospi": [], "kosdaq": []},
        "individual": {"kospi": [], "kosdaq": []},
    }


def collect_for_date(date: str):
    data_by_investor = empty_investor_market_data()
    warnings = []

    for investor_key, investor_name in INVESTORS.items():
        for market in MARKETS:
            try:
                records = fetch_netbuy_top20(date, market, investor_key, investor_name)
                market_key = market.lower()
                data_by_investor[investor_key][market_key] = records

                if not records:
                    warnings.append(f"{date} {market} {investor_name} 데이터가 비어 있습니다.")

            except Exception as exc:
                warnings.append(f"{date} {market} {investor_name} 조회 실패: {str(exc)}")

    return data_by_investor, warnings


def main():
    PUBLIC_DIR.mkdir(exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    warnings = []

    recent_dates = fetch_recent_available_days(count=4)

    if not recent_dates:
        raise RuntimeError("최근 거래일을 찾지 못했습니다.")

    today_trade_date = recent_dates[0]
    yesterday_trade_date = recent_dates[1] if len(recent_dates) > 1 else None

    recent_by_date = {}

    for date in recent_dates:
        data, date_warnings = collect_for_date(date)
        recent_by_date[date] = data
        warnings.extend(date_warnings)

    today_data = recent_by_date[today_trade_date]
    yesterday_data = recent_by_date[yesterday_trade_date] if yesterday_trade_date else empty_investor_market_data()

    all_records = []
    for investor_key in INVESTORS.keys():
        for market_key in ["kospi", "kosdaq"]:
            all_records.extend(today_data[investor_key][market_key])

    summary = build_summary(today_data)
    new_entries = build_new_entries(today_data, yesterday_data)
    three_day_streak = build_three_day_streak(recent_by_date)
    overlap = build_overlap(today_data)

    payload = {
        "success": True,
        "app_name": "KRX 수급 노트",
        "trade_date": today_trade_date,
        "trade_date_text": format_date(today_trade_date),
        "previous_trade_date": yesterday_trade_date,
        "previous_trade_date_text": format_date(yesterday_trade_date) if yesterday_trade_date else None,
        "recent_trade_dates": recent_dates,
        "generated_at": now_iso(),
        "source": "KRX / pykrx",
        "ranking_basis": "NET_BUY_VALUE_TOP20",
        "markets": MARKETS,
        "investors": INVESTORS,

        "pension": today_data["pension"],
        "foreigner": today_data["foreigner"],
        "institution": today_data["institution"],
        "individual": today_data["individual"],

        "summary": summary,
        "new_entries": new_entries,
        "three_day_streak": three_day_streak,
        "overlap": overlap,

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
    print(f"[INFO] previous_trade_date={payload['previous_trade_date']}")

    for investor_key, investor_name in INVESTORS.items():
        kospi_count = len(today_data[investor_key]["kospi"])
        kosdaq_count = len(today_data[investor_key]["kosdaq"])
        print(f"[INFO] {investor_name}: KOSPI={kospi_count}, KOSDAQ={kosdaq_count}")


if __name__ == "__main__":
    main()
