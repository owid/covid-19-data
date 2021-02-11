from typing import List

import pandas as pd


def ensure_integers(input: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    return input.assign(**{col: input[col].astype(int) for col in columns})


def read_coverage(source: str) -> pd.DataFrame:
    return pd.read_csv(
        source,
        usecols=["week_end", "numtotal_atleast1dose", "numtotal_2doses", "prename"],
    )


def filter_canada(input: pd.DataFrame) -> pd.DataFrame:
    return input[input["prename"] == "Canada"]


def coverage_rename_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.rename(
        columns={
            "prename": "location",
            "week_end": "date",
            "numtotal_atleast1dose": "people_vaccinated",
            "numtotal_2doses": "people_fully_vaccinated",
        }
    )


def coverage_pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(filter_canada)
        .pipe(coverage_rename_columns)
        .pipe(ensure_integers, ["people_vaccinated", "people_fully_vaccinated"])
    )


def get_coverage():
    source = "https://health-infobase.canada.ca/src/data/covidLive/vaccination-coverage-map.csv"
    return read_coverage(source).pipe(coverage_pipeline)


def read_doses(source: str) -> pd.DataFrame:
    return pd.read_csv(
        source, usecols=["report_date", "numtotal_all_administered", "prename"]
    )


def doses_rename_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.rename(
        columns={
            "prename": "location",
            "report_date": "date",
            "numtotal_all_administered": "total_vaccinations",
        }
    )


def doses_pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return input.pipe(filter_canada).pipe(doses_rename_columns)


def get_doses() -> pd.DataFrame:
    source = "https://health-infobase.canada.ca/src/data/covidLive/vaccination-administration.csv"
    return read_doses(source).pipe(doses_pipeline)


def read() -> pd.DataFrame:
    return (
        pd.merge(get_coverage(), get_doses(), on=["location", "date"], how="outer")
        .reset_index(drop=True)
        .sort_values("date")
    )


def rectify_total_vaccinations(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        total_vaccinations=input.total_vaccinations.where(
            input.people_fully_vaccinated == 0, input.total_vaccinations
        )
    )


def enrich_vaccine_name(input: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine_name(date: str) -> str:
        if date >= "2020-12-31":
            return "Moderna, Pfizer/BioNTech"
        return "Pfizer/BioNTech"

    return input.assign(vaccine=input.date.apply(_enrich_vaccine_name))


def set_source(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        source_url="https://health-infobase.canada.ca/covid-19/vaccination-coverage/"
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(rectify_total_vaccinations)
        .pipe(enrich_vaccine_name)
        .pipe(set_source)
    )


def main():
    read().pipe(pipeline).to_csv("automations/output/Canada.csv", index=False)


if __name__ == "__main__":
    main()
