import datetime
import re
import pandas as pd


def clean_count(count):
    count = re.sub(r"[^0-9]", "", count)
    count = int(count)
    return count


def clean_date(date, fmt):
    date = pd.to_datetime(date, format=fmt)
    date = str(date.date())
    return date


def increment(location, total_vaccinations, date, vaccine, source_url):

    assert type(location) == str
    assert type(total_vaccinations) == int
    assert type(date) == str
    assert re.match(r"\d{4}-\d{2}-\d{2}", date)
    assert date <= str(datetime.date.today())
    assert type(vaccine) == str
    assert type(source_url) == str

    prev = pd.read_csv(f"automations/output/{location}.csv")

    if total_vaccinations <= prev["total_vaccinations"].max():
        return None

    elif date == prev["date"].max():
        df = prev.copy()
        df.loc[df["date"] == date, "total_vaccinations"] = total_vaccinations

    else:
        new = pd.DataFrame({
            "location": location,
            "date": date,
            "vaccine": vaccine,
            "total_vaccinations": [total_vaccinations],
            "source_url": source_url,
        })
        df = pd.concat([prev, new]).sort_values("date")

    df.to_csv(f"automations/output/{location}.csv", index=False)
