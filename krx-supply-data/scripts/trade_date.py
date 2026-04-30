from datetime import datetime, timedelta
import pytz
from pykrx import stock


KST = pytz.timezone("Asia/Seoul")


def today_kst() -> datetime:
    return datetime.now(KST)


def get_latest_trade_date() -> str:
    """
    가장 최근 거래일을 YYYYMMDD 문자열로 반환한다.
    GitHub Actions가 한국시간 아침에 실행되므로 보통 전 거래일이 반환된다.
    """
    now = today_kst()
    target = now.date() - timedelta(days=1)

    for _ in range(10):
        ymd = target.strftime("%Y%m%d")
        try:
            tickers = stock.get_market_ticker_list(ymd, market="KOSPI")
            if tickers:
                return ymd
        except Exception:
            pass

        target = target - timedelta(days=1)

    raise RuntimeError("최근 거래일을 찾지 못했습니다.")