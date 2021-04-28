import datetime

import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source)


def melt(df: pd.DataFrame) -> pd.DataFrame:
    return df.melt(["Type", "Dose"], var_name="date", value_name="value")


def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df.Type != "Total") & (df.value > 0)]


def pivot(df: pd.DataFrame) -> pd.DataFrame:
    return df.pivot(index=["Type", "date"], columns="Dose", values="value").reset_index()


def enrich_vaccinations(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.assign(total_vaccinations=df.First.fillna(0) + df.Second.fillna(0))
        .rename(columns={"First": "people_vaccinated", "Second": "people_fully_vaccinated"})
    )


def rename_vaccines(df: pd.DataFrame) -> pd.DataFrame:
    vaccine_mapping = {
        "Pfizer": "Pfizer/BioNTech",
        "Sinovac": "Sinovac",
    }
    assert set(df["Type"].unique()) == set(vaccine_mapping.keys())
    return df.replace(vaccine_mapping)


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(melt)
        .pipe(filter_rows)
        .pipe(pivot)
        .pipe(enrich_vaccinations)
        .pipe(rename_vaccines)
    )


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .sort_values("Type")
        .groupby("date", as_index=False)
        .agg(
            people_vaccinated=("people_vaccinated", "sum"),
            people_fully_vaccinated=("people_fully_vaccinated", "sum"),
            total_vaccinations=("total_vaccinations", "sum"),
            vaccine=("Type", ", ".join),
        )
    )


def enrich_metadata(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        source_url="https://www.gob.cl/yomevacuno/",
        location="Chile",
    )


def postprocess_vaccinations(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(aggregate)
        .pipe(enrich_metadata)
    )


def postprocess_manufacturer(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df[["Type", "date", "total_vaccinations"]]
        .rename(columns={"Type": "vaccine"})
        .assign(location="Chile")
    )


def main():
    source = "https://github.com/juancri/covid19-vaccination/raw/master/output/chile-vaccination-type.csv"
    destination = "output/Chile.csv"
    data = read(source).pipe(preprocess)

    condition = (datetime.datetime.now() - pd.to_datetime(data.date.max())).days < 3
    assert condition, "External repository is not up to date"

    data.pipe(postprocess_vaccinations).to_csv(destination, index=False)
    data.pipe(postprocess_manufacturer).to_csv(destination.replace("output", "output/by_manufacturer"), index=False)


if __name__ == "__main__":
    main()
