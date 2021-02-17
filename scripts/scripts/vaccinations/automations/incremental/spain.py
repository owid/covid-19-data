import requests
from bs4 import BeautifulSoup
import pandas as pd
import vaxutils


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    for a in soup.find(class_="menuCCAES").find_all("a"):
        if ".ods" in a["href"]:
            url = "https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/" + a["href"]

    df = pd.read_excel(url)
    keys = ("date", "total_vaccinations", "people_fully_vaccinated", "source_url")
    values = (parse_date(df), parse_total_vaccinations(df),
              parse_people_fully_vaccinated(df), url)
    data = dict(zip(keys, values))
    return pd.Series(data=data)


def parse_date(df: dict) -> str:
    date = str(df["Fecha de la última vacuna registrada (2)"].max().date())
    return date


def parse_total_vaccinations(df: dict) -> int:
    total_vaccinations = int(df.loc[df["Unnamed: 0"] == "Totales", "Dosis administradas (2)"].values[0])
    return total_vaccinations


def parse_people_fully_vaccinated(df: dict) -> int:
    people_fully_vaccinated = int(
        df.loc[df["Unnamed: 0"] == "Totales", "Nº Personas vacunadas(pauta completada)"].values[0])
    return people_fully_vaccinated


def add_totals(input: pd.Series) -> pd.Series:
    people_vaccinated = input['total_vaccinations'] - input['people_fully_vaccinated']
    return vaxutils.enrich_data(input, 'people_vaccinated', people_vaccinated)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Spain")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(add_totals)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
    )


def main():
    source = "https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/vacunaCovid19.htm"
    data = read(source).pipe(pipeline)
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
