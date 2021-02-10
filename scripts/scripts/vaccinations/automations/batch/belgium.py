import os
import pandas as pd
import tempfile


def read(source: str) -> pd.DataFrame:
    """Reading directly with `pd.read_csv` doesn’t work for some reason.
    cURL manages to get the file so we use it and put the result in a temporary folder.
    However it would break on a device that doesn’t have it.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file = f"{temp_dir}/data.csv"
        os.system(f"curl {source} -s -o {temp_file}")
        return pd.read_csv(temp_file, usecols=["date", "first_dose", "second_dose"])


def rename_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.rename(
        columns={
            "first_dose": "people_vaccinated",
            "second_dose": "people_fully_vaccinated",
        }
    )


def add_totals(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        people_vaccinated=input.people_vaccinated.cumsum(),
        people_fully_vaccinated=input.people_fully_vaccinated.cumsum(),
    ).assign(
        total_vaccinations=lambda df: df.people_vaccinated + df.people_fully_vaccinated
    )


def aggregate(input: pd.DataFrame) -> pd.DataFrame:
    return input.groupby("date", as_index=False).sum().sort_values("date")


def set_vaccine(input: pd.DataFrame) -> pd.DataFrame:
    def _set_vaccine(date: str) -> str:
        if date >= "2021-02-08":
            return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        if date >= "2021-01-17":
            return "Moderna, Pfizer/BioNTech"
        return "Pfizer/BioNTech"

    return input.assign(vaccine=input.date.apply(_set_vaccine))


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(location="Belgium", source_url="https://covid-vaccinatie.be/en")


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(rename_columns)
        .pipe(aggregate)
        .pipe(add_totals)
        .pipe(set_vaccine)
        .pipe(enrich_columns)
    )


def main():
    source = "https://covid-vaccinatie.be/api/v1/administered.csv"
    destination = "automations/output/Belgium.csv"

    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
