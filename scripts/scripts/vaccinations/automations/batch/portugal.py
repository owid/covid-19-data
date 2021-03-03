import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, usecols=["data", "doses", "doses1", "doses2"])


def rename_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.rename(
        columns={
            "data": "date",
            "doses": "total_vaccinations",
            "doses1": "people_vaccinated",
            "doses2": "people_fully_vaccinated",
        }
    )


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        date=pd.to_datetime(input.date, format="%d-%m-%Y").astype(str)
    )


def enrich_vaccine_name(input: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine_name(date: str) -> str:
        if date >= "2021-02-09":
            return "Moderna, Pfizer/BioNTech"
        return "Pfizer/BioNTech"

    return input.assign(vaccine=input.date.apply(_enrich_vaccine_name))


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Portugal",
        source_url="https://github.com/dssg-pt/covid19pt-data"
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(rename_columns)
        .pipe(format_date)
        .pipe(enrich_vaccine_name)
        .pipe(enrich_columns)
        .sort_values("date")
    )


def main():
    source = "https://github.com/dssg-pt/covid19pt-data/raw/master/vacinas.csv"
    destination = "automations/output/Portugal.csv"

    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
