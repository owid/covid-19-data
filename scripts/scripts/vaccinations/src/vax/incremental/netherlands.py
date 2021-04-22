import os
from datetime import datetime, timedelta
import re
import requests
import pytz

import dateparser
import pandas as pd
from bs4 import BeautifulSoup

from vax.utils.utils import get_soup
from vax.utils.incremental import enrich_data, increment, clean_count


def read(source_daily: str, source_weekly: str) -> pd.DataFrame:
    # Daily
    soup_daily = get_soup(source_daily)
    date_daily = parse_date_daily(soup_daily)
    total_vaccinations_d = parse_data_daily(soup_daily)
    
    # Weekly
    soup_weekly = get_soup(source_weekly)
    date_weekly = parse_date_weekly(soup_weekly)
    total_vaccinations_w, people_vaccinated, people_fully_vaccinated = parse_data_weekly(soup_weekly)
    
    df = pd.DataFrame.from_records([
        {
            "date": date_weekly,
            "total_vaccinations": total_vaccinations_w,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "source_url": source_weekly
        },
        {
            "date": date_daily,
            "total_vaccinations": total_vaccinations_d,
            "source_url": source_daily
        }
    ])
    return df

def parse_date_weekly(soup: BeautifulSoup):
    for h2 in soup.find_all("h2"):
        date = re.search(r"\d+\s\w+ 202\d", h2.text).group(0)
        if date:
            date = dateparser.parse(date, languages=["nl"])
            break
    return str(date.date())


def parse_data_weekly(soup: BeautifulSoup) -> str:
    df = pd.read_html(str(soup.find("table")), thousands=".")[0]
    # Get total column
    col_total = [col for col in df.columns if "Totaal" in col]
    if len(col_total) != 1:
        raise Exception("Table changed!")
    col_total = col_total[0]

    people_vaccinated = int(df.loc[df.Doelgroep == "Totaal", "Eerste dosis"].item())
    people_fully_vaccinated = int(df.loc[df.Doelgroep == "Totaal", "Tweede dosis"].item())
    total_vaccinations = int(df.loc[df.Doelgroep == "Totaal", col_total].item())

    return total_vaccinations, people_vaccinated, people_fully_vaccinated


def parse_data_daily(soup: BeautifulSoup):
    return clean_count(soup.find(class_="sc-hKwCoD liLttJ").text)


def parse_date_daily(soup: BeautifulSoup):
    date_str = soup.find(class_="sc-hKwCoD jMAwtT").text
    return datetime.strptime(date_str, "Value of %d %B %Y").strftime("%Y-%m-%d")


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Netherlands")


def enrich_vaccine(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def _contains_weekly_record(df: pd.DataFrame, date: str):
    ds = df.loc[df.date == "2021-04-19"]
    if (not ds.empty) and (not ds.isnull().any(axis=1).item()):
        return True
    return False


def merge_with_current_data(df: pd.DataFrame, filepath: str) -> pd.DataFrame:
    col_ints = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]
    # Load current data
    if os.path.isfile(filepath):
        df_current = pd.read_csv(filepath)
        # Merge with weekly data
        df_week = pd.DataFrame(df.loc[0]).T
        df_current = df_current[~df_current.date.isin(df_week.date)]
        df_current = pd.concat([df_week, df_current])
        # Merge with daily data
        df_day = pd.DataFrame(df.loc[1]).T
        if not _contains_weekly_record(df_current, df_day.date):
            df_current = df_current[~df_current.date.isin(df_day.date)]
            df_current = pd.concat([df_day, df_current])
    # Int values
    df_current[col_ints] = df_current[col_ints].astype("Int64").fillna(pd.NA)
    return df_current.sort_values(by="date")


def main():
    source_daily = "https://coronadashboard.government.nl/landelijk/vaccinaties"
    source_weekly = "https://www.rivm.nl/covid-19-vaccinatie/cijfers-vaccinatieprogramma"
    output_file = "output/Netherlands.csv"
    df = read(source_daily, source_weekly).pipe(pipeline)
    df = merge_with_current_data(df, output_file)
    df.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()
