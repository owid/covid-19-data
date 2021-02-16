import requests
from bs4 import BeautifulSoup
import pandas as pd
import vaxutils
import tabula


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    pdf_path = soup.find("a", class_="btn-primary")["href"]  # Get path to newest pdf
    dfs_from_pdf = tabula.read_pdf(pdf_path, pages="all")
    df = pd.DataFrame(dfs_from_pdf[2])  # Hardcoded table location
    values = sorted(pd.to_numeric(df["Unnamed: 2"].str.replace(r"[^\d]", "", regex=True)).dropna().astype(int))
    assert len(values) == 3
    data = {'date': parse_date(df), 'total_vaccinations': parse_total_vaccinations(values),
            'people_vaccinated': parse_people_vaccinated(values),
            'people_fully_vaccinated': parse_people_fully_vaccinated(values), 'source_url': pdf_path}

    return pd.Series(data=data)


def parse_date(df: pd.DataFrame) -> str:
    date = df["Unnamed: 1"].str.replace("Journée du ", "").values[0]
    date = vaxutils.clean_date(date, "%d.%m.%Y")
    return date


def parse_total_vaccinations(values: pd.DataFrame) -> str:
    total_vaccinations = values[2]
    return total_vaccinations


def parse_people_fully_vaccinated(values: pd.DataFrame) -> str:
    people_fully_vaccinated = values[0]
    return people_fully_vaccinated


def parse_people_vaccinated(values: pd.DataFrame) -> str:
    people_vaccinated = values[1]
    return people_vaccinated


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Luxembourg")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(enrich_location)
            .pipe(enrich_vaccine)
    )


def main():
    source = "https://data.public.lu/fr/datasets/covid-19-rapports-journaliers/#_"
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
