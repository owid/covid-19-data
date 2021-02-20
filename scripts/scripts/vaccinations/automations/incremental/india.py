import re
import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import vaxutils


def read(soup: BeautifulSoup) -> pd.Series:
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    arr = []
    url = ""
    for a in soup.find_all("a"):
        if "COVID-19 Vaccination" in a.text:
            url = "https://pib.gov.in" + a["href"]

    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    table = soup.find(class_="Table")
    spans = table("tr")[-1].findAll('span', {'style': 'font-family:Times New Roman,Times,serif'})
    for span in spans:
        arr.append(span.get_text())

    keys = ("date", "people_vaccinated", "people_fully_vaccinated", "total_vaccinations", "source_url")
    values = (parse_date(soup), vaxutils.clean_count(arr[1]), vaxutils.clean_count(arr[2]),
              vaxutils.clean_count(arr[3]), url)
    data = dict(zip(keys, values))
    return pd.Series(data=data)


def parse_date(soup: BeautifulSoup) -> str:
    text = soup.find(class_="ReleaseDateSubHeaddateTime").get_text().rstrip()
    regex = r"(.*?) by PIB Delhi"
    dt = str(re.findall(regex, text)).split()
    day = datetime.datetime.strptime(dt[1], "%d").day
    month = datetime.datetime.strptime(dt[2], "%b").month
    year = datetime.datetime.strptime(dt[3], "%Y").year
    date = str(month) + "-" + str(day) + "-" + str(year)
    date = vaxutils.clean_date(date, "%m-%d-%Y")
    return date


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "India")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Covaxin, Oxford/AstraZeneca")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(enrich_location)
            .pipe(enrich_vaccine)
    )


def main():
    source = "https://pib.gov.in/AllReleasem.aspx"
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    if "COVID-19 Vaccination" in soup.text:
        data = read(soup).pipe(pipeline)
        vaxutils.increment(
            location=data['location'],
            total_vaccinations=data['total_vaccinations'],
            people_vaccinated=data['people_vaccinated'],
            people_fully_vaccinated=data['people_fully_vaccinated'],
            date=data['date'],
            source_url=data['source_url'],
            vaccine=data['vaccine']
        )


if __name__ == "__main__":
    main()
