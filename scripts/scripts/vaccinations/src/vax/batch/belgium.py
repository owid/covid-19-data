import pandas as pd

from vax.utils.pipeline import enrich_total_vaccinations


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, usecols=["DATE", "DOSE", "COUNT"])


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["DATE", "DOSE"], as_index=False)
        .sum()
        .sort_values("DATE")
        .pivot(index="DATE", columns="DOSE", values="COUNT")
        .reset_index()
        .fillna(0)
    )


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "DATE": "date",
        }
    )


def add_totals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        total_vaccinations=df.A + df.B + df.C,
        people_vaccinated=df.A + df.C,
        people_fully_vaccinated=df.B + df.C,
    )
    return df.drop(columns=["A", "B", "C"])


def enrich_cumsum(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        total_vaccinations=df.total_vaccinations.cumsum().astype(int),
        people_vaccinated=df.people_vaccinated.cumsum().astype(int),
        people_fully_vaccinated=df.people_fully_vaccinated.cumsum().astype(int),
    )


def enrich_vaccine_name(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine_name(date: str) -> str:
        # See timeline in:
        # https://datastudio.google.com/embed/u/0/reporting/c14a5cfc-cab7-4812-848c-0369173148ab/page/hOMwB
        if date < "2021-01-11":
            return "Pfizer/BioNTech"
        elif "2021-01-11" <= date < "2021-02-12":
            return "Moderna, Pfizer/BioNTech"
        elif "2021-02-12" <= date < "2021-04-28":
            return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        elif "2021-04-28" <= date:
            return "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))


def enrich_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        location="Belgium",
        source_url="https://epistat.wiv-isp.be/covid/"
    )


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .pipe(aggregate)
        .pipe(rename_columns)
        .pipe(add_totals)
        .pipe(enrich_cumsum)
        .pipe(enrich_vaccine_name)
        .pipe(enrich_columns)
    )


def main():
    source = "https://epistat.sciensano.be/Data/COVID19BE_VACC.csv"
    destination = "output/Belgium.csv"

    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
