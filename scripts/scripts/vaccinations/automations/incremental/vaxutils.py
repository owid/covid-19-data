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


def increment(
        location,
        total_vaccinations,
        date,
        vaccine,
        source_url,
        people_vaccinated=None,
        people_fully_vaccinated=None
    ):

    assert type(location) == str
    assert type(total_vaccinations) == int
    assert type(people_vaccinated) == int or people_vaccinated is None
    assert type(people_fully_vaccinated) == int or people_fully_vaccinated is None
    assert type(date) == str
    assert re.match(r"\d{4}-\d{2}-\d{2}", date)
    assert date <= str(datetime.date.today() + datetime.timedelta(days=1))
    assert type(vaccine) == str
    assert type(source_url) == str

    prev = pd.read_csv(f"automations/output/{location}.csv")

    if total_vaccinations <= prev["total_vaccinations"].max():
        return None

    elif date == prev["date"].max():
        df = prev.copy()
        df.loc[df["date"] == date, "total_vaccinations"] = total_vaccinations
        df.loc[df["date"] == date, "source_url"] = source_url

    else:
        new = pd.DataFrame({
            "location": location,
            "date": date,
            "vaccine": vaccine,
            "total_vaccinations": [total_vaccinations],
            "source_url": source_url,
        })
        if people_vaccinated is not None:
            new["people_vaccinated"] = people_vaccinated
        if people_fully_vaccinated is not None:
            new["people_fully_vaccinated"] = people_fully_vaccinated
        df = pd.concat([prev, new]).sort_values("date")

    df.to_csv(f"automations/output/{location}.csv", index=False)

    print(f"NEW: {total_vaccinations} doses on {date}")
