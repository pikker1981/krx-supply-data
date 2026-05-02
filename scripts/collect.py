import json
import traceback
from pathlib import Path
from datetime import datetime
import pytz

from trade_date import get_latest_trade_date
from pension_streak import compute_pension_streak


KST = pytz.timezone("Asia/Seoul")

PUBLIC_DIR = Path("public")
HISTORY_DIR = PUBLIC_DIR / "history"


def now_iso() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def main():
    PUBLIC_DIR.mkdir(exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    latest_trade_date = get_latest_trade_date()

    trade_date_text = f"{latest_trade_date[:4]}-{latest_trade_date[4:6]}-{latest_trade_date[6:]}"

    warnings = [
        "테스트 버전입니다. 아직 실제 수급 데이터는 수집하지 않습니다."
    ]

    # 연기금 연속 순매수 (오늘 기준 과거 N영업일)
    try:
        pension_streak = compute_pension_streak(
            latest_trade_date, min_streak=7, lookback=30
        )
        print(f"[pension_streak] {len(pension_streak)} stocks (>=7일 연속)")
    except Exception as exc:
        print("[pension_streak] FAILED:", exc)
        traceback.print_exc()
        pension_streak = []
        warnings.append(f"pension_streak 계산 실패: {exc}")

    payload = {
        "success": True,
        "app_name": "KRX 수급 노트",
        "trade_date": trade_date_text,
        "generated_at": now_iso(),
        "source": "KRX / pykrx",
        "ranking_basis": "NET_BUY_VALUE_TOP20",
        "markets": ["KOSPI", "KOSDAQ"],
        "investor_ranks": [],
        "combined_netbuy": [],
        "pension_streak": pension_streak,
        "warnings": warnings,
    }

    latest_path = PUBLIC_DIR / "latest.json"
    history_path = HISTORY_DIR / f"{trade_date_text}.json"

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