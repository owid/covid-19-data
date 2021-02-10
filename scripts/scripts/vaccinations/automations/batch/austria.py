import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, sep=";")


def check_columns(input: pd.DataFrame, expected) -> pd.DataFrame:
    n_columns = input.shape[1]
    if n_columns != expected:
        raise ValueError(
            "The provided input does not have {} columns. It has {} columns".format(
                expected, n_columns
            )
        )
    return input


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
        location="Austria",
        source_url="https://info.gesundheitsministerium.gv.at/opendata/",
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech",
    )


def pipeline_vaccinations(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(check_columns, expected=6)
        .pipe(filter_country)
        .pipe(select_columns, columns=["Datum", "GemeldeteImpfungenLaender"])
        .pipe(rename_columns, columns={
            "Datum": "date",
            "GemeldeteImpfungenLaender": "total_vaccinations",
        })
        .pipe(format_date)
        .dropna()
    )


def pipeline_people(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(check_columns, expected=14)
        .pipe(filter_country)
        .pipe(select_columns, columns=["Datum", "Teilgeimpfte", "Vollimmunisierte"])
        .pipe(rename_columns, columns={
            "Datum": "date",
            "Teilgeimpfte": "people_vaccinated",
            "Vollimmunisierte": "people_fully_vaccinated",
        })
        .pipe(format_date)
        .dropna()
    )


def main():
    source_vaccinations = "https://info.gesundheitsministerium.gv.at/data/timeline-bundeslaendermeldungen.csv"
    source_people = "https://info.gesundheitsministerium.gv.at/data/timeline.csv"
    destination = "automations/output/Austria.csv"

    vaccinations = read(source_vaccinations).pipe(pipeline_vaccinations)
    people = read(source_people).pipe(pipeline_people)

    (
        pd.merge(vaccinations, people, on="date", how="outer")
        .pipe(enrich_columns)
        .sort_values("date")
        .to_csv(destination, index=False)
    )


if __name__ == "__main__":
    main()
