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
    data = {'date': parse_date(df), 'total_vaccinations': parse_total_vaccinations(df),
            'people_fully_vaccinated': parse_people_fully_vaccinated(df), 'source_url': url}

    return pd.Series(data=data)


def parse_date(df: pd.DataFrame) -> str:
    date = str(df["Fecha de la última vacuna registrada (2)"].max().date())
    return date


def parse_total_vaccinations(df: pd.DataFrame) -> str:
    total_vaccinations = int(df.loc[df["Unnamed: 0"] == "Totales", "Dosis administradas (2)"].values[0])
    return total_vaccinations


def parse_people_fully_vaccinated(df: pd.DataFrame) -> str:
    people_fully_vaccinated = int(
        df.loc[df["Unnamed: 0"] == "Totales", "Nº Personas vacunadas(pauta completada)"].values[0])
    return people_fully_vaccinated


def add_totals(input: pd.Series) -> pd.Series:
    people_vaccinated = int(input['total_vaccinations']) - int(input['people_fully_vaccinated'])
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
        location=str(data['location']),
        total_vaccinations=int(data['total_vaccinations']),
        people_vaccinated=int(data['people_vaccinated']),
        people_fully_vaccinated=int(data['people_fully_vaccinated']),
        date=str(data['date']),
        source_url=str(data['source_url']),
        vaccine=str(data['vaccine'])
    )


if __name__ == "__main__":
    main()
