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


def add_metrics(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(people_vaccinated=input.total_vaccinations - input.people_fully_vaccinated)


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(date=pd.to_datetime(input.date, format="%d/%m/%Y").dt.date)


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Malta",
        source_url="https://github.com/COVID19-Malta/COVID19-Cases",
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech",
    )


def exclude_data_points(input: pd.DataFrame) -> pd.DataFrame:
    # The data contains an error that creates a negative change in the people_vaccinated series
    input = input[input.date.astype(str) != "2021-01-24"]
    return input


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(check_columns, expected=3)
        .pipe(rename_columns, columns={
            "Date": "date",
            "Total Vaccination Doses": "total_vaccinations",
            " Second Dose Taken": "people_fully_vaccinated",
        })
        .pipe(add_metrics)
        .pipe(format_date)
        .pipe(enrich_columns)
        .pipe(exclude_data_points)
    )


def main():
    source = "https://github.com/COVID19-Malta/COVID19-Cases/raw/master/COVID-19%20Malta%20-%20Vaccination%20Data.csv"
    destination = "output/Malta.csv"
    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
