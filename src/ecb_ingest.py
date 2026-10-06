"""Daily EUR exchange rates from the ECB Data Portal (official reference rates).

ECB series key: EXR / D.<CUR>.EUR.SP00.A  →  units of <CUR> per 1 EUR.
Example: 1 EUR = 5.10 BRL  →  price_eur = price_brl / 5.10

Used in two ways:
  * locally:      python src/ecb_ingest.py   (writes data/ecb_brl_eur_2016_2018.csv)
  * in Databricks: notebooks/01_bronze imports fetch_rates (falls back to the CSV in data/ if the
                   workspace has no outbound internet, e.g. Databricks Free Edition)
"""
from __future__ import annotations

import argparse
import io
from datetime import date

import pandas as pd
import requests

ECB_URL = "https://data-api.ecb.europa.eu/service/data/EXR/D.{cur}.EUR.SP00.A"


def parse_ecb_csv(csv_text: str, currency: str) -> pd.DataFrame:
    """Parse the ECB 'csvdata' response into columns: rate_date, currency, rate_per_eur."""
    raw = pd.read_csv(io.StringIO(csv_text))
    missing = {"TIME_PERIOD", "OBS_VALUE"} - set(raw.columns)
    if missing:
        raise ValueError(f"Unexpected ECB format, missing columns: {missing}")
    out = pd.DataFrame(
        {
            "rate_date": pd.to_datetime(raw["TIME_PERIOD"]).dt.date,
            "currency": currency.upper(),
            "rate_per_eur": pd.to_numeric(raw["OBS_VALUE"], errors="coerce"),
        }
    )
    out = out.dropna(subset=["rate_per_eur"])
    return out.sort_values("rate_date").reset_index(drop=True)


def fetch_rates(currency: str, start: date | str, end: date | str, timeout: int = 30) -> pd.DataFrame:
    """Download daily reference rates for one currency between start and end (inclusive)."""
    params = {"format": "csvdata", "startPeriod": str(start), "endPeriod": str(end)}
    resp = requests.get(ECB_URL.format(cur=currency.upper()), params=params, timeout=timeout)
    resp.raise_for_status()
    if not resp.text.strip():
        return pd.DataFrame(columns=["rate_date", "currency", "rate_per_eur"])
    return parse_ecb_csv(resp.text, currency)


def fill_calendar_days(rates: pd.DataFrame) -> pd.DataFrame:
    """ECB publishes no rates on weekends/holidays. Forward-fill so every calendar day has a rate.

    Adds column `is_filled` = True for days that received the previous business day's rate.
    """
    if rates.empty:
        return rates.assign(is_filled=pd.Series(dtype=bool))
    df = rates.copy()
    df["rate_date"] = pd.to_datetime(df["rate_date"])
    full_index = pd.date_range(df["rate_date"].min(), df["rate_date"].max(), freq="D")
    df = df.set_index("rate_date").reindex(full_index)
    df["is_filled"] = df["rate_per_eur"].isna()
    df["rate_per_eur"] = df["rate_per_eur"].ffill()
    df["currency"] = df["currency"].ffill()
    df = df.rename_axis("rate_date").reset_index()
    df["rate_date"] = df["rate_date"].dt.date
    return df[["rate_date", "currency", "rate_per_eur", "is_filled"]]


def main() -> None:
    p = argparse.ArgumentParser(description="Download ECB daily reference rates")
    p.add_argument("--currency", default="BRL")
    p.add_argument("--start", default="2016-01-01")
    p.add_argument("--end", default="2018-12-31")
    p.add_argument("--out", default="data/ecb_brl_eur_2016_2018.csv")
    args = p.parse_args()

    # raw business days only – the forward-fill for weekends happens in silver (src/transforms.fx_daily)
    rates = fetch_rates(args.currency, args.start, args.end)
    rates.to_csv(args.out, index=False)
    print(f"{len(rates):,} ECB business days written to {args.out}")


if __name__ == "__main__":
    main()
