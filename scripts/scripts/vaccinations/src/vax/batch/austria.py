import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, sep=";")


def filter_country(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["Name"] == "Österreich"]


def select_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    return df[columns]


def rename_columns(df: pd.DataFrame, columns: dict) -> pd.DataFrame:
    return df.rename(columns=columns)


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(date=df.date.str.slice(0, 10))


def enrich_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated,
        location="Austria",
        source_url="https://info.gesundheitsministerium.gv.at/opendata/",
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech",
    )


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
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
    destination = "output/Austria.csv"
    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
