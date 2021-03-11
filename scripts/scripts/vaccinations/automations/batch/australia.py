import requests

import pandas as pd


def read(source: str) -> pd.DataFrame:
    data = requests.get(source)
    return pd.read_json(source)


def filter_rows(input: pd.DataFrame) -> pd.DataFrame:
    return input[(input.NAME == "Australia") & input.VACC_DOSE_CNT.notnull()]


def rename_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input[["REPORT_DATE", "VACC_DOSE_CNT", "VACC_PEOPLE_CNT"]].rename(
        columns={
            "REPORT_DATE": "date",
            "VACC_DOSE_CNT": "total_vaccinations",
            "VACC_PEOPLE_CNT": "people_vaccinated",
        }
    )


def enrich_vaccinations(input: pd.DataFrame) -> pd.DataFrame:
    input.loc[input.date < "2021-03-14", "people_vaccinated"] = input.total_vaccinations
    input["people_fully_vaccinated"] = input.total_vaccinations - input.people_vaccinated
    return input


def enrich_vaccine_name(input: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine(date: str) -> str:
        if date >= "2021-03-08":
            return "Oxford/AstraZeneca, Pfizer/BioNTech"
        return "Pfizer/BioNTech"
    return input.assign(vaccine=input.date.astype(str).apply(_enrich_vaccine))


def enrich_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Australia",
        source_url="https://covidlive.com.au/vaccinations"
    )


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input
        .pipe(filter_rows)
        .pipe(rename_columns)
        .pipe(enrich_vaccinations)
        .pipe(enrich_vaccine_name)
        .pipe(enrich_columns)
        .sort_values("date")
    )


def main():
    source = "https://covidlive.com.au/covid-live.json"
    destination = "automations/output/Australia.csv"

    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
