import json
from pathlib import Path
from datetime import datetime, timedelta

import pytz
from pykrx import stock


KST = pytz.timezone("Asia/Seoul")
DOCS_LATEST_PATH = Path("docs/latest.json")


def _today_kst() -> datetime:
    return datetime.now(KST)


def _read_existing_latest_trade_date() -> str | None:
    """
    GitHub Actions에서 KRX/pykrx 응답이 일시적으로 실패할 때,
    기존 docs/latest.json의 거래일을 fallback으로 사용한다.
    """
    if not DOCS_LATEST_PATH.exists():
        return None

    try:
        payload = json.loads(DOCS_LATEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None

    trade_date = payload.get("trade_date")

    if isinstance(trade_date, str) and len(trade_date) == 8 and trade_date.isdigit():
        return trade_date

    recent = payload.get("recent_trade_dates") or []
    if recent and isinstance(recent[0], str) and len(recent[0]) == 8:
        return recent[0]

    return None


def _has_market_data(yyyymmdd: str) -> bool:
    """
    해당 일자에 KOSPI/KOSDAQ 데이터가 실제로 조회되는지 확인한다.
    pykrx가 빈 응답 또는 JSON 오류를 내는 경우 False 처리한다.
    """
    for market in ("KOSPI", "KOSDAQ"):
        try:
            df = stock.get_market_ohlcv_by_ticker(yyyymmdd, market=market)

            if df is not None and not df.empty:
                return True

        except Exception as exc:
            print(f"[trade_date] {yyyymmdd} {market} 조회 실패: {exc}")

    return False


def get_latest_trade_date(max_lookback_days: int = 20) -> str:
    """
    최신 거래일을 찾는다.

    1순위: 오늘부터 과거로 내려가며 pykrx 데이터 조회
    2순위: 기존 docs/latest.json의 trade_date
    3순위: 실패 처리
    """
    today = _today_kst().date()

    for offset in range(max_lookback_days + 1):
        d = today - timedelta(days=offset)
        yyyymmdd = d.strftime("%Y%m%d")

        # 주말은 우선 건너뛰되, 휴일/임시휴장 판단은 데이터 조회로 처리
        if d.weekday() >= 5:
            continue

        if _has_market_data(yyyymmdd):
            print(f"[trade_date] latest={yyyymmdd}")
            return yyyymmdd

    fallback = _read_existing_latest_trade_date()

    if fallback:
        print(f"[trade_date] fallback latest={fallback}")
        return fallback

    raise RuntimeError("최근 거래일을 찾지 못했습니다.")
