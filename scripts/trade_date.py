from datetime import datetime, timedelta

import pytz
from pykrx import stock


KST = pytz.timezone("Asia/Seoul")


def _today_kst():
    return datetime.now(KST).date()


def _has_ohlcv_data(yyyymmdd: str) -> bool:
    """
    실제 장이 열린 거래일인지 OHLCV 데이터로 확인한다.
    투자자별 수급 조회 함수는 휴장일/빈 응답에서 pykrx 내부 오류 로그를 뱉을 수 있으므로,
    거래일 판정에는 사용하지 않는다.
    """
    for market in ("KOSPI", "KOSDAQ"):
        try:
            df = stock.get_market_ohlcv_by_ticker(yyyymmdd, market=market)

            if df is not None and not df.empty:
                return True

        except Exception as exc:
            print(f"[trade_date] {yyyymmdd} {market} OHLCV 조회 실패: {exc}")

    return False


def get_latest_trade_date(max_lookback_days: int = 30) -> str:
    """
    오늘부터 과거로 내려가며 실제 OHLCV 데이터가 있는 최신 거래일을 찾는다.
    """
    today = _today_kst()

    for offset in range(max_lookback_days + 1):
        d = today - timedelta(days=offset)

        # 주말은 우선 제외
        if d.weekday() >= 5:
            continue

        yyyymmdd = d.strftime("%Y%m%d")

        if _has_ohlcv_data(yyyymmdd):
            print(f"[trade_date] latest={yyyymmdd}")
            return yyyymmdd

    raise RuntimeError("최근 거래일을 찾지 못했습니다.")


def get_recent_trade_dates(
    latest_trade_date: str | None = None,
    count: int = 30,
    max_scan_days: int = 120,
) -> list[str]:
    """
    latest_trade_date 기준 최근 거래일 목록을 반환한다.
    휴장일은 OHLCV 데이터가 없으므로 제외된다.
    """
    if latest_trade_date:
        base = datetime.strptime(latest_trade_date, "%Y%m%d").date()
    else:
        base = _today_kst()

    dates = []

    for offset in range(max_scan_days + 1):
        d = base - timedelta(days=offset)

        if d.weekday() >= 5:
            continue

        yyyymmdd = d.strftime("%Y%m%d")

        if _has_ohlcv_data(yyyymmdd):
            dates.append(yyyymmdd)

        if len(dates) >= count:
            break

    if not dates:
        raise RuntimeError("최근 거래일 목록을 찾지 못했습니다.")

    print(f"[trade_date] recent={dates[:5]} ... total={len(dates)}")
    return dates
