import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, sep=";")


def filter_country(input: pd.DataFrame) -> pd.DataFrame:
    return input[input["Name"] == "Österreich"]


def select_columns(input: pd.DataFrame, columns: list) -> pd.DataFrame:
    return input[columns]


def rename_columns(input: pd.DataFrame, columns: dict) -> pd.DataFrame:
    return input.rename(columns=columns)


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(date=input.date.str.slice(0, 10))


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        total_vaccinations=input.people_vaccinated + input.people_fully_vaccinated,
        location="Austria",
        source_url="https://info.gesundheitsministerium.gv.at/opendata/",
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech",
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input
        .pipe(filter_country)
        .pipe(select_columns, columns=["Datum", "Teilgeimpfte", "Vollimmunisierte"])
        .pipe(rename_columns, columns={
            "Datum": "date",
            "Teilgeimpfte": "people_vaccinated",
            "Vollimmunisierte": "people_fully_vaccinated",
        })
        .pipe(format_date)
        .pipe(enrich_columns)
        .sort_values("date")
    )


def main():
    source = "https://info.gesundheitsministerium.gv.at/data/timeline-eimpfpass.csv"
    destination = "automations/output/Austria.csv"
    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
