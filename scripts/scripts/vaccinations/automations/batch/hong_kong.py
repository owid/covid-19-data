import requests
import pandas as pd


def read(source:str) -> pd.DataFrame:
    data = requests.get(source).json()
    return pd.DataFrame.from_dict([
        {
            "date": d["date"],
            "people_vaccinated": d["firstDose"]["cumulative"]["total"],
            "people_fully_vaccinated": d["secondDose"]["cumulative"]["total"]
        } for d in data
    ])


def enrich_vaccinations(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated
    )


def enrich_vaccine(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine(date: str) -> str:
        if date < "2021-03-06":
            return "Sinovac"
        return "Pfizer/BioNTech, Sinovac"
    return df.assign(vaccine=df.date.apply(_enrich_vaccine))


def enrich_metadata(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Hong Kong",
        source_url="https://www.covidvaccine.gov.hk/en/dashboard"
    )


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .pipe(enrich_vaccinations)
        .pipe(enrich_vaccine)
        .pipe(enrich_metadata)
    )


def main():
    source = "https://static.data.gov.hk/covid-vaccine/bar_vaccination_date.json"
    destination = "automations/output/Hong Kong.csv"

    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()