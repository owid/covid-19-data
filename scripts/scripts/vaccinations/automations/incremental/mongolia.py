import requests
import pandas as pd
import vaxutils


def read(source: str) -> pd.Series:
    data = requests.get(source).json()
    return parse_data(data)


def parse_data(data: dict) -> pd.Series:

    date = vaxutils.clean_date(data["updated"], "%Y/%m/%d")

    total_vaccinations = data["progress"]
    
    return pd.Series(data={
        "date": date,
        "total_vaccinations": total_vaccinations,
    })


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "location", "Mongolia")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "vaccine", "Oxford/AstraZeneca")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "source_url", "https://ikon.mn/")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main():
    source = "https://ikon.mn/api/json/vaccine"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=str(data["location"]),
        total_vaccinations=int(data["total_vaccinations"]),
        date=str(data["date"]),
        source_url=str(data["source_url"]),
        vaccine=str(data["vaccine"])
    )


if __name__ == "__main__":
    main()
