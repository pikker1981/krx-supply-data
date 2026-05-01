import argparse
import json
import traceback

from trade_date import get_latest_trade_date, get_recent_trade_dates
from collect import (
    HISTORY_DIR,
    collect_all_investor_data,
    build_payload,
    add_pension_streak,
    flatten_records,
    history_path_for,
)


def write_history_only(payload: dict, trade_date: str) -> None:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    path = history_path_for(trade_date)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[DONE] {path}")


def backfill(days: int, force: bool = False) -> None:
    latest_trade_date = get_latest_trade_date()
    target_dates = get_recent_trade_dates(
        latest_trade_date=latest_trade_date,
        count=days,
    )

    print(f"[backfill] target_dates={target_dates}")

    # 오래된 날짜부터 생성해야 신규/연속 계산이 조금 더 자연스럽다.
    for trade_date in reversed(target_dates):
        path = history_path_for(trade_date)

        if path.exists() and not force:
            print(f"[SKIP] exists: {path}")
            continue

        warnings = []

        try:
            print(f"[collect] {trade_date}")

            recent_trade_dates = get_recent_trade_dates(
                latest_trade_date=trade_date,
                count=30,
            )

            current_data = collect_all_investor_data(
                trade_date=trade_date,
                warnings=warnings,
            )

            all_records = flatten_records(current_data)

            if not all_records:
                print(f"[SKIP] {trade_date}: 수급 데이터 0건")
                continue

            payload = build_payload(
                latest_trade_date=trade_date,
                recent_trade_dates=recent_trade_dates,
                current_data=current_data,
                warnings=warnings,
            )

            add_pension_streak(payload, warnings)

            write_history_only(payload, trade_date)

        except Exception as exc:
            print(f"[FAILED] {trade_date}: {exc}")
            traceback.print_exc()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=20)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    backfill(days=args.days, force=args.force)


if __name__ == "__main__":
    main()
