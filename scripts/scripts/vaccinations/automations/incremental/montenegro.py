import requests
import pandas as pd
import vaxutils


def read(source: str) -> pd.Series:
    data = requests.get(source).json()
    return parse_data(data)


def parse_data(data: dict) -> pd.Series:

    people_vaccinated = int(data["data"][data["sheetNames"].index("1. doza")][0][0])

    people_fully_vaccinated = data["data"][data["sheetNames"].index("2. doza")]
    if len(people_fully_vaccinated) > 0:
        people_fully_vaccinated = int(people_fully_vaccinated[0][0])
    else:
        people_fully_vaccinated = 0

    total_vaccinations = people_vaccinated + people_fully_vaccinated

    date = str(pd.to_datetime(data["refreshed"], unit="ms").date())

    data = {
        "date": date,
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
    }
    return pd.Series(data=data)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "location", "Montenegro")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "vaccine", "Sinopharm/Beijing, Sputnik V")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "source_url", "https://www.covidodgovor.me/")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "https://atlas.jifo.co/api/connectors/520021dc-c292-4903-9cdb-a2467f64ed97"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=str(data["location"]),
        total_vaccinations=int(data["total_vaccinations"]),
        people_vaccinated=int(data["people_vaccinated"]),
        people_fully_vaccinated=int(data["people_fully_vaccinated"]),
        date=str(data["date"]),
        source_url=str(data["source_url"]),
        vaccine=str(data["vaccine"])
    )


if __name__ == "__main__":
    main()
