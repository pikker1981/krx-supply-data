import json
from pathlib import Path
from datetime import datetime, timedelta

import pytz
from pykrx import stock


KST = pytz.timezone("Asia/Seoul")
DOCS_DIR = Path("docs")
HISTORY_DIR = DOCS_DIR / "history"
LATEST_PATH = DOCS_DIR / "latest.json"


INVESTOR_CHECKS = [
    ("KOSPI", "외국인"),
    ("KOSPI", "기관합계"),
    ("KOSDAQ", "외국인"),
    ("KOSDAQ", "기관합계"),
]


def today_kst():
    return datetime.now(KST).date()


def payload_has_records(payload: dict | None) -> bool:
    if not payload:
        return False

    all_records = payload.get("all_records")
    if isinstance(all_records, list) and len(all_records) > 0:
        return True

    for investor_key in ("pension", "foreigner", "institution", "individual"):
        for market_key in ("kospi", "kosdaq"):
            rows = payload.get(investor_key, {}).get(market_key, [])
            if isinstance(rows, list) and rows:
                return True

    return False


def read_json(path: Path) -> dict | None:
    if not path.exists():
        return None

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def history_trade_dates() -> list[str]:
    if not HISTORY_DIR.exists():
        return []

    dates = []

    for path in HISTORY_DIR.glob("*-investor-netbuy.json"):
        trade_date = path.name.split("-", 1)[0]

        if len(trade_date) != 8 or not trade_date.isdigit():
            continue

        payload = read_json(path)
        if payload_has_records(payload):
            dates.append(trade_date)

    dates = sorted(set(dates), reverse=True)
    return dates


def latest_valid_history_date() -> str | None:
    dates = history_trade_dates()

    if dates:
        return dates[0]

    latest = read_json(LATEST_PATH)
    if payload_has_records(latest):
        trade_date = latest.get("trade_date")
        if isinstance(trade_date, str) and len(trade_date) == 8 and trade_date.isdigit():
            return trade_date

    return None


def is_valid_trade_date(yyyymmdd: str) -> bool:
    """
    실제 투자자별 순매수 데이터가 존재하는 거래일인지 확인한다.
    휴장일·KRX 빈 응답·pykrx JSON 오류는 False 처리한다.
    """
    for market, investor in INVESTOR_CHECKS:
        try:
            df = stock.get_market_net_purchases_of_equities(
                yyyymmdd,
                yyyymmdd,
                market,
                investor,
            )

            if df is not None and not df.empty:
                return True

        except Exception as exc:
            print(f"[trade_date] {yyyymmdd} {market}/{investor} 조회 실패: {exc}")

    return False


def get_latest_trade_date(max_lookback_days: int = 30) -> str:
    """
    오늘부터 과거로 내려가며 실제 투자자별 수급 데이터가 있는 최신 거래일을 찾는다.
    찾지 못하면 비어 있지 않은 history/latest 파일의 최근 거래일을 fallback으로 사용한다.
    """
    base = today_kst()

    for offset in range(max_lookback_days + 1):
        day = base - timedelta(days=offset)

        if day.weekday() >= 5:
            continue

        yyyymmdd = day.strftime("%Y%m%d")

        if is_valid_trade_date(yyyymmdd):
            print(f"[trade_date] latest={yyyymmdd}")
            return yyyymmdd

    fallback = latest_valid_history_date()

    if fallback:
        print(f"[trade_date] fallback latest={fallback}")
        return fallback

    raise RuntimeError("최근 거래일을 찾지 못했습니다.")


def get_recent_trade_dates(latest_trade_date: str | None = None, count: int = 30) -> list[str]:
    """
    실제 투자자별 수급 데이터가 존재하는 최근 거래일 목록을 반환한다.
    pykrx 조회 실패 시 history에 저장된 정상 파일도 함께 활용한다.
    """
    if latest_trade_date:
        base = datetime.strptime(latest_trade_date, "%Y%m%d").date()
    else:
        base = today_kst()

    dates = []

    for offset in range(0, 140):
        day = base - timedelta(days=offset)

        if day.weekday() >= 5:
            continue

        yyyymmdd = day.strftime("%Y%m%d")

        if is_valid_trade_date(yyyymmdd):
            dates.append(yyyymmdd)

        if len(dates) >= count:
            break

    history_dates = [d for d in history_trade_dates() if d <= base.strftime("%Y%m%d")]
    merged = sorted(set(dates + history_dates), reverse=True)

    if not merged:
        raise RuntimeError("최근 거래일 목록을 찾지 못했습니다.")

    print(f"[trade_date] recent={merged[:5]} ... total={len(merged[:count])}")
    return merged[:count]
