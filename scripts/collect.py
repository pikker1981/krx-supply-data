import json
import os
from pathlib import Path
from datetime import datetime
import pytz


KST = pytz.timezone("Asia/Seoul")

PUBLIC_DIR = Path("docs")
HISTORY_DIR = PUBLIC_DIR / "history"


def now_iso() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def main():
    api_key = os.environ.get("KRX_API_KEY")

    if not api_key:
        raise RuntimeError("KRX_API_KEY 환경 변수가 설정되지 않았습니다.")

    PUBLIC_DIR.mkdir(exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "success": True,
        "app_name": "KRX 수급 노트",
        "trade_date": None,
        "generated_at": now_iso(),
        "source": "KRX OPEN API",
        "ranking_basis": "NET_BUY_VALUE_TOP20",
        "markets": ["KOSPI", "KOSDAQ"],
        "investor_ranks": [],
        "combined_netbuy": [],
        "pension_streak": [],
        "warnings": [
            "KRX_API_KEY GitHub Secret 연결 테스트 성공.",
            "아직 실제 KRX API endpoint는 연결하지 않았습니다."
        ]
    }

    latest_path = PUBLIC_DIR / "latest.json"
    history_path = HISTORY_DIR / "api-key-test.json"

    latest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    history_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("[OK] KRX_API_KEY is set.")
    print(f"[DONE] {latest_path}")
    print(f"[DONE] {history_path}")


if __name__ == "__main__":
    main()
