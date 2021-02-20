import datetime

import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source)


def melt(input: pd.DataFrame) -> pd.DataFrame:
    return input.melt(["Type", "Dose"], var_name="date", value_name="value")


def filter_rows(input: pd.DataFrame) -> pd.DataFrame:
    return input[(input.Type != "Total") & (input.value > 0)]


def pivot(input: pd.DataFrame) -> pd.DataFrame:
    return input.pivot(index=["Type", "date"], columns="Dose", values="value").reset_index()


def enrich_vaccinations(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.assign(total_vaccinations=input.First.fillna(0) + input.Second.fillna(0))
        .rename(columns={"First": "people_vaccinated", "Second": "people_fully_vaccinated"})
    )


def rename_vaccines(input: pd.DataFrame) -> pd.DataFrame:
    vaccine_mapping = {
        "Pfizer": "Pfizer/BioNTech",
        "Sinovac": "Sinovac",
    }
    assert set(input["Type"].unique()) == set(vaccine_mapping.keys())
    return input.replace(vaccine_mapping)


def preprocess(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(melt)
        .pipe(filter_rows)
        .pipe(pivot)
        .pipe(enrich_vaccinations)
        .pipe(rename_vaccines)
    )


def aggregate(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input
        .sort_values("Type")
        .groupby("date", as_index=False)
        .agg(
            people_vaccinated=("people_vaccinated", "sum"),
            people_fully_vaccinated=("people_fully_vaccinated", "sum"),
            total_vaccinations=("total_vaccinations", "sum"),
            vaccine=("Type", ", ".join),
        )
    )


def enrich_metadata(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        source_url="https://www.gob.cl/yomevacuno/",
        location="Chile",
    )


def postprocess_vaccinations(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(aggregate)
        .pipe(enrich_metadata)
    )


def postprocess_manufacturer(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input[["Type", "date", "total_vaccinations"]]
        .rename(columns={"Type": "vaccine"})
        .assign(location="Chile")
    )


def main():
    source = "https://github.com/juancri/covid19-vaccination/raw/master/output/chile-vaccination-type.csv"
    destination = "automations/output/Chile.csv"
    data = read(source).pipe(preprocess)

    assert (datetime.datetime.now() - pd.to_datetime(data.date.max())).days < 3, \
    "External repository is not up to date"

    data.pipe(postprocess_vaccinations).to_csv(destination, index=False)
    data.pipe(postprocess_manufacturer).to_csv(destination.replace("output", "output/by_manufacturer"), index=False)


if __name__ == "__main__":
    main()
