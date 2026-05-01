from datetime import datetime, timedelta

import pytz
from pykrx import stock


KST = pytz.timezone("Asia/Seoul")


def _today_kst():
    return datetime.now(KST).date()


def _is_valid_trade_date(yyyymmdd: str) -> bool:
    """
    실제 투자자별 순매수 데이터가 존재하는 거래일인지 확인한다.
    휴장일이면 pykrx가 빈 데이터프레임을 주거나 예외를 낼 수 있으므로 False 처리한다.
    """
    checks = [
        ("KOSPI", "외국인"),
        ("KOSPI", "기관합계"),
        ("KOSDAQ", "외국인"),
        ("KOSDAQ", "기관합계"),
    ]

    for market, investor in checks:
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
    """
    today = _today_kst()

    for offset in range(max_lookback_days + 1):
        d = today - timedelta(days=offset)

        # 주말은 건너뜀
        if d.weekday() >= 5:
            continue

        yyyymmdd = d.strftime("%Y%m%d")

        if _is_valid_trade_date(yyyymmdd):
            print(f"[trade_date] latest={yyyymmdd}")
            return yyyymmdd

    raise RuntimeError("최근 거래일을 찾지 못했습니다.")


def get_recent_trade_dates(latest_trade_date: str | None = None, count: int = 30) -> list[str]:
    """
    실제 투자자별 수급 데이터가 존재하는 최근 거래일 목록을 반환한다.
    """
    if latest_trade_date:
        base = datetime.strptime(latest_trade_date, "%Y%m%d").date()
    else:
        base = _today_kst()

    dates = []

    for offset in range(0, 120):
        d = base - timedelta(days=offset)

        if d.weekday() >= 5:
            continue

        yyyymmdd = d.strftime("%Y%m%d")

        if _is_valid_trade_date(yyyymmdd):
            dates.append(yyyymmdd)

        if len(dates) >= count:
            break

    if not dates:
        raise RuntimeError("최근 거래일 목록을 찾지 못했습니다.")

    print(f"[trade_date] recent={dates[:5]} ... total={len(dates)}")
    return dates
