import json
import traceback
from pathlib import Path
from datetime import datetime, timedelta

import pytz
from pykrx import stock

from trade_date import get_latest_trade_date
from pension_streak import compute_pension_streak


KST = pytz.timezone("Asia/Seoul")

PUBLIC_DIR = Path("docs")
HISTORY_DIR = PUBLIC_DIR / "history"

MARKETS = {
    "kospi": "KOSPI",
    "kosdaq": "KOSDAQ",
}

INVESTORS = {
    "pension": "연기금",
    "foreigner": "외국인",
    "institution": "기관합계",
    "individual": "개인",
}


def now_iso() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def to_date_text(yyyymmdd: str) -> str:
    if not yyyymmdd or len(yyyymmdd) != 8:
        return ""
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"


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


def safe_get(row, *keys, default=0):
    for key in keys:
        if key in row:
            return row[key]
    return default


def get_recent_trade_dates(latest_trade_date: str, count: int = 30) -> list[str]:
    """
    latest_trade_date 기준 과거 영업일 목록을 만든다.
    pykrx 캘린더 함수 의존을 줄이기 위해 KOSPI OHLCV 조회 성공 여부로 거래일을 판정한다.
    """
    dates = []
    base = datetime.strptime(latest_trade_date, "%Y%m%d").date()

    for offset in range(0, 90):
        d = base - timedelta(days=offset)
        yyyymmdd = d.strftime("%Y%m%d")

        try:
            df = stock.get_market_ohlcv(yyyymmdd, market="KOSPI")
            if df is not None and not df.empty:
                dates.append(yyyymmdd)
        except Exception:
            continue

        if len(dates) >= count:
            break

    return dates


def collect_investor_market(trade_date: str, market_key: str, investor_key: str, top_n: int = 20) -> list[dict]:
    market = MARKETS[market_key]
    investor = INVESTORS[investor_key]

    df = stock.get_market_net_purchases_of_equities(
        trade_date,
        trade_date,
        market,
        investor,
    )

    if df is None or df.empty:
        return []

    df = df.copy()

    # pykrx 결과 컬럼명 기준:
    # 종목명, 매도거래량, 매수거래량, 순매수거래량, 매도거래대금, 매수거래대금, 순매수거래대금
    net_value_col = "순매수거래대금"
    if net_value_col not in df.columns:
        # 컬럼명이 바뀌어도 최대한 방어
        possible = [c for c in df.columns if "순매수" in str(c) and "대금" in str(c)]
        if possible:
            net_value_col = possible[0]
        else:
            return []

    df[net_value_col] = df[net_value_col].apply(to_int)
    df = df[df[net_value_col] > 0]
    df = df.sort_values(net_value_col, ascending=False).head(top_n)

    rows = []
    for idx, (ticker, row) in enumerate(df.iterrows(), start=1):
        row_dict = row.to_dict()

        name = safe_get(row_dict, "종목명", "ISU_ABBRV", default="")
        if not name:
            try:
                name = stock.get_market_ticker_name(str(ticker))
            except Exception:
                name = ""

        item = {
            "rank": idx,
            "trade_date": trade_date,
            "market": market,
            "investor_key": investor_key,
            "investor": investor,
            "ISU_SRT_CD": str(ticker),
            "ISU_ABBRV": str(name),
            "NETBID_TRDVAL": to_int(safe_get(row_dict, "순매수거래대금", "NETBID_TRDVAL")),
            "NETBID_TRDVOL": to_int(safe_get(row_dict, "순매수거래량", "NETBID_TRDVOL")),
            "ASK_TRDVAL": to_int(safe_get(row_dict, "매도거래대금", "ASK_TRDVAL")),
            "BID_TRDVAL": to_int(safe_get(row_dict, "매수거래대금", "BID_TRDVAL")),
            "ASK_TRDVOL": to_int(safe_get(row_dict, "매도거래량", "ASK_TRDVOL")),
            "BID_TRDVOL": to_int(safe_get(row_dict, "매수거래량", "BID_TRDVOL")),
        }
        rows.append(item)

    return rows


def collect_all_investor_data(trade_date: str, warnings: list[str]) -> dict:
    data = {}

    for investor_key in INVESTORS:
        data[investor_key] = {}

        for market_key in MARKETS:
            try:
                rows = collect_investor_market(trade_date, market_key, investor_key, top_n=20)
                data[investor_key][market_key] = rows
                print(f"[{investor_key}/{market_key}] {len(rows)} rows")
            except Exception as exc:
                traceback.print_exc()
                data[investor_key][market_key] = []
                warnings.append(f"{investor_key}/{market_key} 수집 실패: {exc}")

    return data


def build_summary(data: dict) -> dict:
    summary = {}

    for investor_key in INVESTORS:
        summary[investor_key] = {}

        for market_key in MARKETS:
            rows = data.get(investor_key, {}).get(market_key, [])
            total = sum(to_int(r.get("NETBID_TRDVAL")) for r in rows)
            top = rows[0] if rows else {}

            summary[investor_key][market_key] = {
                "count": len(rows),
                "total_netbuy_value": total,
                "top_stock_name": top.get("ISU_ABBRV", "-"),
                "top_stock_code": top.get("ISU_SRT_CD", ""),
                "top_netbuy_value": to_int(top.get("NETBID_TRDVAL")),
            }

    return summary


def history_path_for(trade_date: str) -> Path:
    return HISTORY_DIR / f"{trade_date}-investor-netbuy.json"


def load_history_payload(trade_date: str) -> dict | None:
    path = history_path_for(trade_date)
    if not path.exists():
        return None

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def code_set(payload: dict | None, investor_key: str, market_key: str) -> set[str]:
    if not payload:
        return set()
    rows = payload.get(investor_key, {}).get(market_key, [])
    return {str(r.get("ISU_SRT_CD")) for r in rows if r.get("ISU_SRT_CD")}


def build_new_entries(current_payload: dict, previous_payload: dict | None) -> dict:
    result = {}

    for investor_key in INVESTORS:
        result[investor_key] = {}

        for market_key in MARKETS:
            current_rows = current_payload.get(investor_key, {}).get(market_key, [])
            prev_codes = code_set(previous_payload, investor_key, market_key)

            if not prev_codes:
                result[investor_key][market_key] = []
                continue

            result[investor_key][market_key] = [
                r for r in current_rows
                if str(r.get("ISU_SRT_CD")) not in prev_codes
            ]

    return result


def build_three_day_streak(current_payload: dict, recent_trade_dates: list[str]) -> dict:
    result = {}

    # 현재일 + 과거 2거래일 필요
    if len(recent_trade_dates) < 3:
        for investor_key in INVESTORS:
            result[investor_key] = {market_key: [] for market_key in MARKETS}
        return result

    prev1 = load_history_payload(recent_trade_dates[1])
    prev2 = load_history_payload(recent_trade_dates[2])

    for investor_key in INVESTORS:
        result[investor_key] = {}

        for market_key in MARKETS:
            current_rows = current_payload.get(investor_key, {}).get(market_key, [])
            codes1 = code_set(prev1, investor_key, market_key)
            codes2 = code_set(prev2, investor_key, market_key)

            if not codes1 or not codes2:
                result[investor_key][market_key] = []
                continue

            result[investor_key][market_key] = [
                r for r in current_rows
                if str(r.get("ISU_SRT_CD")) in codes1 and str(r.get("ISU_SRT_CD")) in codes2
            ]

    return result


def build_overlap(current_payload: dict) -> dict:
    result = {}

    for market_key, market_name in MARKETS.items():
        bucket = {}

        for investor_key, investor_name in INVESTORS.items():
            rows = current_payload.get(investor_key, {}).get(market_key, [])

            for row in rows:
                code = str(row.get("ISU_SRT_CD", ""))
                if not code:
                    continue

                if code not in bucket:
                    bucket[code] = {
                        "ISU_SRT_CD": code,
                        "ISU_ABBRV": row.get("ISU_ABBRV", ""),
                        "market": market_name,
                        "investors": [],
                        "total_netbuy_value": 0,
                    }

                bucket[code]["investors"].append(investor_name)
                bucket[code]["total_netbuy_value"] += to_int(row.get("NETBID_TRDVAL"))

        rows = [
            item for item in bucket.values()
            if len(item["investors"]) >= 2
        ]

        rows.sort(key=lambda x: to_int(x.get("total_netbuy_value")), reverse=True)
        result[market_key] = rows

    return result


def flatten_records(current_payload: dict) -> list[dict]:
    records = []

    for investor_key in INVESTORS:
        for market_key in MARKETS:
            records.extend(current_payload.get(investor_key, {}).get(market_key, []))

    return records


def main():
    PUBLIC_DIR.mkdir(exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    warnings = []

    latest_trade_date = get_latest_trade_date()
    recent_trade_dates = get_recent_trade_dates(latest_trade_date, count=30)
    previous_trade_date = recent_trade_dates[1] if len(recent_trade_dates) > 1 else None

    current_data = collect_all_investor_data(latest_trade_date, warnings)
    previous_payload = load_history_payload(previous_trade_date) if previous_trade_date else None

    summary = build_summary(current_data)
    new_entries = build_new_entries(current_data, previous_payload)
    three_day_streak = build_three_day_streak(current_data, recent_trade_dates)
    overlap = build_overlap(current_data)
    all_records = flatten_records(current_data)

    payload = {
        "success": True,
        "app_name": "KRX 수급 노트",
        "trade_date": latest_trade_date,
        "trade_date_text": to_date_text(latest_trade_date),
        "previous_trade_date": previous_trade_date,
        "previous_trade_date_text": to_date_text(previous_trade_date) if previous_trade_date else None,
        "recent_trade_dates": recent_trade_dates[:30],
        "generated_at": now_iso(),
        "source": "KRX / pykrx",
        "ranking_basis": "NET_BUY_VALUE_TOP20",
        "markets": ["KOSPI", "KOSDAQ"],
        "investors": INVESTORS,
        "summary": summary,
        "new_entries": new_entries,
        "three_day_streak": three_day_streak,
        "overlap": overlap,
        "all_records": all_records,
        "warnings": warnings,
    }

    # 기존 index.html이 기대하는 최상위 구조 유지
    for investor_key in INVESTORS:
        payload[investor_key] = current_data.get(investor_key, {})

    # 연기금 7일 이상 연속 순매수
    try:
        pension_streak_result = compute_pension_streak(
            latest_payload=payload,
            history_dir=HISTORY_DIR,
            min_streak=7,
            lookback=30,
        )

        payload["seven_day_streak"] = {
            "pension": {
                "kospi": pension_streak_result.get("kospi", []),
                "kosdaq": pension_streak_result.get("kosdaq", []),
            }
        }

        # 호환용 flat list
        payload["pension_streak"] = (
            pension_streak_result.get("kospi", [])
            + pension_streak_result.get("kosdaq", [])
        )

        print(
            "[pension_streak]",
            len(payload["seven_day_streak"]["pension"]["kospi"]),
            len(payload["seven_day_streak"]["pension"]["kosdaq"]),
        )

    except Exception as exc:
        traceback.print_exc()
        payload["seven_day_streak"] = {"pension": {"kospi": [], "kosdaq": []}}
        payload["pension_streak"] = []
        payload["warnings"].append(f"연기금 연속 순매수 계산 실패: {exc}")

    latest_path = PUBLIC_DIR / "latest.json"
    history_path = history_path_for(latest_trade_date)

    latest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    history_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[DONE] {latest_path}")
    print(f"[DONE] {history_path}")


if __name__ == "__main__":
    main()
