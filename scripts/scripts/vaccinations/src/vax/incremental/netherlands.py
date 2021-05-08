import os
from datetime import datetime
import re
import unicodedata

import dateparser
import pandas as pd
from bs4 import BeautifulSoup

from vax.utils.utils import get_soup
from vax.utils.incremental import clean_count


def read(source_daily: str, source_weekly: str) -> pd.DataFrame:
    # Daily
    # soup_daily = get_soup(source_daily)
    # for div in soup_daily.find_all("div"):
    #     if div.text == "Vaccine doses administered":
    #         dose_block = div.parent.findChildren()[1]
    #         break
    # date_daily = parse_date_daily(dose_block)
    # total_vaccinations_d = parse_data_daily(dose_block)

    # Weekly
    soup_weekly = get_soup(source_weekly)
    date_weekly = parse_date_weekly(soup_weekly)
    total_vaccinations, people_vaccinated, _ = parse_data_weekly(soup_weekly)

    return pd.DataFrame.from_records([
        {
            "date": date_weekly,
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            # "people_fully_vaccinated": people_fully_vaccinated,
            "source_url": source_weekly
        }
    ])


def parse_date_weekly(soup: BeautifulSoup):
    for h2 in soup.find_all("h2"):
        date = re.search(r"\d+\s\w+ 202\d", unicodedata.normalize("NFKD", h2.text)).group(0)
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


# def parse_data_daily(element):
#     return clean_count(element.find("span").text)


# def parse_date_daily(element):
#     date_str = element.find_all("span")[1].text
#     return datetime.strptime(date_str, "Value of %d %B %Y").strftime("%Y-%m-%d")


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Netherlands")


def enrich_vaccine(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(vaccine="Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


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
        # df_day = pd.DataFrame(df.loc[1]).T
        # if not _contains_weekly_record(df_current, df_day.date):
        #     df_current = df_current[~df_current.date.isin(df_day.date)]
        #     df_current = pd.concat([df_day, df_current])
    # Int values
    df_current[col_ints] = df_current[col_ints].astype("Int64").fillna(pd.NA)
    return df_current.sort_values(by="date")


def main(paths):
    source_daily = "https://coronadashboard.government.nl/landelijk/vaccinaties"
    source_weekly = "https://www.rivm.nl/covid-19-vaccinatie/cijfers-vaccinatieprogramma"
    output_file = paths.out_tmp("Netherlands")
    df = read(source_daily, source_weekly).pipe(pipeline)
    df = merge_with_current_data(df, output_file)
    df.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()
