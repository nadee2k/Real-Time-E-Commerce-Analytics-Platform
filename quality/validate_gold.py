"""Simple data quality checks for Gold KPI parquet outputs."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

GOLD_PATH = Path(os.getenv("GOLD_PATH", "data/gold/kpis"))


def run_checks(df: pd.DataFrame) -> list[str]:
    failures: list[str] = []

    required = {"window", "active_users", "orders", "revenue"}
    missing = required - set(df.columns)
    if missing:
        failures.append(f"Missing columns: {sorted(missing)}")

    for col in ["active_users", "orders", "revenue"]:
        if col in df.columns and (df[col].fillna(0) < 0).any():
            failures.append(f"Negative values found in {col}")

    if "active_users" in df.columns and "orders" in df.columns:
        invalid = (df["orders"].fillna(0) > df["active_users"].fillna(0)).any()
        if invalid:
            failures.append("Orders exceed active users in at least one window")

    return failures


def main() -> None:
    files = sorted(GOLD_PATH.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found at {GOLD_PATH}")

    df = pd.read_parquet(files)
    failures = run_checks(df)
    if failures:
        raise SystemExit("Quality checks failed:\n- " + "\n- ".join(failures))

    print(f"Quality checks passed for {len(df)} rows")


if __name__ == "__main__":
    main()
