from datetime import date

import pytest

from src.ecb_ingest import fill_calendar_days, parse_ecb_csv

# Minimal ECB-style response with real reference rates (Fri 5 Jan, Mon 8 Jan 2018) + one empty value
SAMPLE = """KEY,FREQ,CURRENCY,TIME_PERIOD,OBS_VALUE
EXR.D.BRL.EUR.SP00.A,D,BRL,2018-01-05,3.9057
EXR.D.BRL.EUR.SP00.A,D,BRL,2018-01-08,3.8825
EXR.D.BRL.EUR.SP00.A,D,BRL,2018-01-09,
"""


def test_parse_keeps_valid_rows_sorted():
    df = parse_ecb_csv(SAMPLE, "brl")
    assert list(df.columns) == ["rate_date", "currency", "rate_per_eur"]
    assert len(df) == 2  # empty OBS_VALUE dropped
    assert df["currency"].unique().tolist() == ["BRL"]
    assert df["rate_date"].iloc[0] == date(2018, 1, 5)


def test_parse_rejects_unknown_format():
    with pytest.raises(ValueError):
        parse_ecb_csv("a,b\n1,2\n", "BRL")


def test_weekend_is_forward_filled():
    df = fill_calendar_days(parse_ecb_csv(SAMPLE, "BRL"))
    # Fri 5th, Sat 6th, Sun 7th, Mon 8th
    assert len(df) == 4
    saturday = df[df["rate_date"] == date(2018, 1, 6)].iloc[0]
    assert saturday["rate_per_eur"] == pytest.approx(3.9057)
    assert bool(saturday["is_filled"]) is True
    monday = df[df["rate_date"] == date(2018, 1, 8)].iloc[0]
    assert bool(monday["is_filled"]) is False
