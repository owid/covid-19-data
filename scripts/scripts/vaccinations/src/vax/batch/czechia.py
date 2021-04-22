"""Since we need to translate vaccine names, we'll check that no new
manufacturers were added, so that we can maintain control over this
"""


import pandas as pd


vaccine_mapping = {
    "Comirnaty": "Pfizer/BioNTech",
    "COVID-19 Vaccine Moderna": "Moderna",
    "VAXZEVRIA": "Oxford/AstraZeneca",
    "COVID-19 Vaccine Janssen": "Johnson&Johnson",
}

one_dose_vaccines = ["Johnson&Johnson"]


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, parse_dates=["datum"])


def check_columns(df: pd.DataFrame) -> pd.DataFrame:
    expected = [
        "datum",
        "vakcina",
        "kraj_nuts_kod",
        "kraj_nazev",
        "vekova_skupina",
        "prvnich_davek",
        "druhych_davek",
        "celkem_davek",
    ]
    if list(df.columns) != expected:
        raise ValueError(
            "Wrong columns. Was expecting {} and got {}".format(
                expected, list(df.columns)
            )
        )
    return df


def check_vaccine_names(df: pd.DataFrame) -> pd.DataFrame:
    unknown_vaccines = set(df.vakcina.unique()).difference(
        set(vaccine_mapping.keys())
    )
    if unknown_vaccines:
        raise ValueError("Found unknown vaccines: {}".format(unknown_vaccines))
    return df


def translate_vaccine_names(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(vaccine_mapping)


def enrich_source(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(source_url="https://onemocneni-aktualne.mzcr.cz/covid-19")


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Czechia")


def enrich_metadata(df: pd.DataFrame) -> pd.DataFrame:
    return df.pipe(enrich_location).pipe(enrich_source)


def base_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(check_columns)
        .pipe(check_vaccine_names)
        .pipe(translate_vaccine_names)
    )


def breakdown_per_vaccine(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(by=["datum", "vakcina"], as_index=False)
        [["celkem_davek"]].sum()
        .sort_values("datum")
        .assign(
            size=lambda df: df.groupby(by=["vakcina"], as_index=False)["celkem_davek"].cumsum()
        )
        .drop("celkem_davek", axis=1)
        .rename(
            columns={
                "datum": "date",
                "vakcina": "vaccine",
                "size": "total_vaccinations",
            }
        )
        .pipe(enrich_location)
    )


def aggregate_by_date_vaccine(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(by=["datum", "vakcina"])
        [["prvnich_davek", "druhych_davek"]].sum()
        .reset_index()
        .rename({
            "prvnich_davek": 1,
            "druhych_davek": 2,
        }, axis=1)
    )


def infer_one_dose_vaccines(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df.vakcina.isin(one_dose_vaccines), 2] = df[1]
    return df


def infer_total_vaccinations(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df.vakcina.isin(one_dose_vaccines), "total_vaccinations"] = df[1].fillna(0)
    df.loc[-df.vakcina.isin(one_dose_vaccines), "total_vaccinations"] = df[1].fillna(0) + df[2].fillna(0)
    return df


def aggregate_by_date(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(by="datum")
        .agg(
            vaccine=("vakcina", lambda x: ", ".join(sorted(set(x)))),
            people_vaccinated=(1, "sum"),  # 1 means 1st dose
            people_fully_vaccinated=(2, "sum"),
            total_vaccinations=("total_vaccinations", "sum"),
        )
        .reset_index()
    )


def check_first_date(df: pd.DataFrame) -> pd.DataFrame:
    first_date = df.date.min()
    expected = "2020-12-27"
    if first_date != expected:
        raise ValueError(
            "Expected the first date to be {}, encountered {}.".format(
                expected, first_date
            )
        )
    return df


def translate_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={"datum": "date"})


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(date=df.date.astype(str).str.slice(0, 10))


def enrich_cumulated_sums(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(by="date").assign(
        **{
            col: df[col].cumsum().astype(int)
            for col in [
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
            ]
        }
    )


def global_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(aggregate_by_date_vaccine)
        .pipe(infer_one_dose_vaccines)
        .pipe(infer_total_vaccinations)
        .pipe(aggregate_by_date)
        .pipe(translate_columns)
        .pipe(format_date)
        .pipe(enrich_cumulated_sums)
        .pipe(enrich_metadata)
        .pipe(check_first_date)
    )


def main():
    source = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani.csv"

    global_output = "output/Czechia.csv"
    by_manufacturer_output = "output/by_manufacturer/Czechia.csv"

    base = read(source).pipe(base_pipeline)

    base.pipe(breakdown_per_vaccine).to_csv(by_manufacturer_output, index=False)
    base.pipe(global_pipeline).to_csv(global_output, index=False)


if __name__ == "__main__":
    main()
