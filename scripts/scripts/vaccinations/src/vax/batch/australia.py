import os
import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_json(source)


def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df.NAME == "Australia") & df.VACC_DOSE_CNT.notnull()]


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df[["REPORT_DATE", "VACC_DOSE_CNT", "VACC_PEOPLE_CNT"]].rename(
        columns={
            "REPORT_DATE": "date",
            "VACC_DOSE_CNT": "total_vaccinations",
            "VACC_PEOPLE_CNT": "people_vaccinated",
        }
    )


def enrich_vaccinations(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df.date < "2021-03-14", "people_vaccinated"] = df.total_vaccinations
    df["people_fully_vaccinated"] = df.total_vaccinations - df.people_vaccinated
    return df


def enrich_vaccine_name(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine(date: str) -> str:
        if date >= "2021-03-08":
            return "Oxford/AstraZeneca, Pfizer/BioNTech"
        return "Pfizer/BioNTech"
    return df.assign(vaccine=df.date.astype(str).apply(_enrich_vaccine))


def enrich_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        location="Australia",
        source_url="https://covidlive.com.au/vaccinations"
    )


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .pipe(filter_rows)
        .pipe(rename_columns)
        .pipe(enrich_vaccinations)
        .pipe(enrich_vaccine_name)
        .pipe(enrich_columns)
        .sort_values("date")
    )


def main(paths):
    source = "https://covidlive.com.au/covid-live.json"
    destination = paths.out_tmp("Australia")

    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
