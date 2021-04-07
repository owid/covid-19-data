import os
import shutil
import datetime
import re
import requests
import tempfile

from bs4 import BeautifulSoup
import pandas as pd


def get_soup(source: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "*",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    return BeautifulSoup(requests.get(source, headers=headers).content, "html.parser")


def read_xlsx_from_url(url: str, as_series: bool) -> pd.DataFrame:
    """Download and load xls file from URL.

    Args:
        url (str): File url.
        as_series: Set to True to return a pandas.Series object. Source file must be of shape 1xN (1 row, N columns).

    Returns:
        pandas.DataFrame: Data loaded.
    """
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux i686)"}
    response = requests.get(url, headers=headers)
    with tempfile.NamedTemporaryFile() as tmp:
        with open(tmp.name, 'wb') as f:
            f.write(response.content)
        df = pd.read_excel(tmp.name)
    if as_series:
        return df.T.squeeze()
    return df


def clean_count(count):
    count = re.sub(r"[^0-9]", "", count)
    count = int(count)
    return count


def clean_date(date, fmt):
    date = pd.to_datetime(date, format=fmt)
    date = str(date.date())
    return date


def enrich_data(input: pd.Series, row, value) -> pd.Series:
    return input.append(pd.Series({row: value}))


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

    filepath_automated = f"automations/output/{location}.csv"
    filepath_public = f"../../../public/data/vaccinations/country_data/{location}.csv"
    filepath = None
    for filepath in [filepath_automated, filepath_public, None]:  # priority order
        if filepath is None or os.path.isfile(filepath):
            df = _increment(
                filepath=filepath,
                location=location,
                total_vaccinations=total_vaccinations,
                date=date,
                vaccine=vaccine,
                source_url=source_url,
                people_vaccinated=people_vaccinated,
                people_fully_vaccinated=people_fully_vaccinated
            )
            break
    df.to_csv(f"automations/output/{location}.csv", index=False)

    #print(f"NEW: {total_vaccinations} doses on {date}")


def _increment(location, total_vaccinations, date, vaccine, source_url, people_vaccinated=None,
               people_fully_vaccinated=None, filepath=None):
    if filepath is None:
        df = _build_df(
            location, total_vaccinations, date, vaccine, source_url, people_vaccinated , people_fully_vaccinated
        )
    else:
        prev = pd.read_csv(filepath)
        if total_vaccinations <= prev["total_vaccinations"].max():
            return None

        elif date == prev["date"].max():
            df = prev.copy()
            df.loc[df["date"] == date, "total_vaccinations"] = total_vaccinations
            df.loc[df["date"] == date, "people_vaccinated"] = people_vaccinated
            df.loc[df["date"] == date, "people_fully_vaccinated"] = people_fully_vaccinated
            df.loc[df["date"] == date, "source_url"] = source_url

        else:
            new = _build_df(
                location, total_vaccinations, date, vaccine, source_url, people_vaccinated , people_fully_vaccinated
            )
            df = pd.concat([prev, new])
    return df.sort_values("date")


def _build_df(location, total_vaccinations, date, vaccine, source_url, people_vaccinated=None,
               people_fully_vaccinated=None):
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
    return new