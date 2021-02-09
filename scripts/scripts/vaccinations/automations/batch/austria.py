import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, sep=";")


def check_columns(input: pd.DataFrame, expected=14) -> pd.DataFrame:
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


def select_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input[["Datum", "EingetrageneImpfungen", "Teilgeimpfte", "Vollimmunisierte"]]


def rename_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.rename(
        columns={
            "Datum": "date",
            "EingetrageneImpfungen": "total_vaccinations",
            "Teilgeimpfte": "people_vaccinated",
            "Vollimmunisierte": "people_fully_vaccinated",
        }
    )


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(date=input.date.str.slice(0, 10))


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Austria",
        source_url="https://www.data.gv.at/katalog/dataset/589132b2-c000-4c60-85b4-c5036cdf3406",
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech",
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(check_columns, expected=14)
        .pipe(filter_country)
        .pipe(select_columns)
        .pipe(rename_columns)
        .pipe(format_date)
        .pipe(enrich_columns)
    )


def main():
    source = "https://info.gesundheitsministerium.gv.at/data/timeline.csv"
    destination = "automations/output/Austria.csv"

    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
