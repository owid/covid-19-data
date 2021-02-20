import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source)


def check_columns(input: pd.DataFrame, expected) -> pd.DataFrame:
    n_columns = input.shape[1]
    if n_columns != expected:
        raise ValueError(
            "The provided input does not have {} columns. It has {} columns".format(
                expected, n_columns
            )
        )
    return input


def rename_columns(input: pd.DataFrame, columns: dict) -> pd.DataFrame:
    return input.rename(columns=columns)


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(date=pd.to_datetime(input.date, format="%d/%m/%Y").dt.date)


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Ecuador",
        source_url="https://github.com/andrab/ecuacovid",
        vaccine="Pfizer/BioNTech",
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(check_columns, expected=4)
        .pipe(rename_columns, columns={
            "fecha": "date",
            "dosis_total": "total_vaccinations",
            "primera_dosis": "people_vaccinated",
            "segunda_dosis": "people_fully_vaccinated",
        })
        .pipe(format_date)
        .pipe(enrich_columns)
    )


def main():
    source = "https://github.com/andrab/ecuacovid/raw/master/datos_crudos/vacunas/vacunas.csv"
    destination = "automations/output/Ecuador.csv"
    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
