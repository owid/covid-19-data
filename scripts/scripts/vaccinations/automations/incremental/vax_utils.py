import re
import pandas as pd

def clean_count(count):
    count = count.replace(",", "")
    count = int(count)
    return count


def increment(location, total_vaccinations, date, vaccine, source_url):

    assert type(location) == str
    assert type(total_vaccinations) == int
    assert type(date) == str
    assert re.match(r"\d{4}-\d{2}-\d{2}", date)
    assert type(vaccine) == str
    assert type(source_url) == str

    prev = pd.read_csv(f"automations/output/{location}.csv")

    if date <= prev["date"].max() or total_vaccinations <= prev["total_vaccinations"].max():
        return None

    new = pd.DataFrame({
        "location": location,
        "date": date,
        "vaccine": vaccine,
        "total_vaccinations": [total_vaccinations],
        "source_url": source_url,
    })

    df = pd.concat([prev, new]).sort_values("date", ascending=False)
    df.to_csv(f"automations/output/{location}.csv", index=False)
