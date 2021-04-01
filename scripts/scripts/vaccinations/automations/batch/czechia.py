"""Since we need to translate vaccine names, we'll check that no new
manufacturers were added, so that we can maintain control over this

IMPORTANT: If a new vaccine is added, see if it requires a single dose
or two doses. If it's a single-dose one, make sure to fix the calculation
of `total_vaccinations`
"""


import pandas as pd

from utils.pipeline import enrich_total_vaccinations


vaccine_mapping = {
    "Comirnaty": "Pfizer/BioNTech",
    "COVID-19 Vaccine Moderna": "Moderna",
    "VAXZEVRIA": "Oxford/AstraZeneca",
}


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, parse_dates=["datum"])


def check_columns(input: pd.DataFrame) -> pd.DataFrame:
    expected = [
        "datum",
        "vakcina",
        "kraj_nuts_kod",
        "kraj_nazev",
        "zarizeni_kod",
        "zarizeni_nazev",
        "poradi_davky",
        "vekova_skupina",
    ]
    if list(input.columns) != expected:
        raise ValueError(
            "Wrong columns. Was expecting {} and got {}".format(
                expected, list(input.columns)
            )
        )
    return input


def check_vaccine_names(input: pd.DataFrame) -> pd.DataFrame:
    unknown_vaccines = set(input.vakcina.unique()).difference(
        set(vaccine_mapping.keys())
    )
    if unknown_vaccines:
        raise ValueError("Found unknown vaccines: {}".format(unknown_vaccines))
    return input


def translate_vaccine_names(input: pd.DataFrame) -> pd.DataFrame:
    return input.replace(vaccine_mapping)


def enrich_source(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(source_url="https://onemocneni-aktualne.mzcr.cz/covid-19")


def enrich_location(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(location="Czechia")


def enrich_metadata(input: pd.DataFrame) -> pd.DataFrame:
    return input.pipe(enrich_location).pipe(enrich_source)


def base_pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(check_columns)
        .pipe(check_vaccine_names)
        .pipe(translate_vaccine_names)
    )


def breakdown_per_vaccine(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.groupby(by=["datum", "vakcina"], as_index=False)
        .size()
        .sort_values("datum")
        .assign(
            size=lambda df: df.groupby(by=["vakcina"], as_index=False)["size"].cumsum()
        )
        .rename(
            columns={
                "datum": "date",
                "vakcina": "vaccine",
                "size": "total_vaccinations",
            }
        )
        .pipe(enrich_location)
    )


def aggregate_by_date_vaccine(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.groupby(by=["datum", "vakcina", "poradi_davky"])
        .size()
        .unstack()
        .reset_index()
    )


def aggregate_by_date(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.groupby(by="datum")
        .agg(
            vaccine=("vakcina", lambda x: ", ".join(sorted(set(x)))),
            people_vaccinated=(1, "sum"),  # 1 means 1st dose
            people_fully_vaccinated=(2, "sum"),
        )
        .reset_index()
    )


def check_first_date(input: pd.DataFrame) -> pd.DataFrame:
    first_date = input.date.min()
    expected = "2020-12-27"
    if first_date != expected:
        raise ValueError(
            "Expected the first date to be {}, encountered {}.".format(
                expected, first_date
            )
        )
    return input


def translate_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.rename(columns={"datum": "date"})


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(date=input.date.astype(str).str.slice(0, 10))


def enrich_cumulated_sums(input: pd.DataFrame) -> pd.DataFrame:
    return input.sort_values(by="date").assign(
        **{
            col: input[col].cumsum().astype(int)
            for col in [
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
            ]
        }
    )


def global_enrichments(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(enrich_total_vaccinations)
        .pipe(enrich_cumulated_sums)
        .pipe(enrich_metadata)
    )


def global_pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(aggregate_by_date_vaccine)
        .pipe(aggregate_by_date)
        .pipe(translate_columns)
        .pipe(format_date)
        .pipe(global_enrichments)
        .pipe(check_first_date)
    )


def main():
    source = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovaci-mista.csv"

    global_output = "automations/output/Czechia.csv"
    by_manufacturer_output = "automations/output/by_manufacturer/Czechia.csv"

    base = read(source).pipe(base_pipeline)

    base.pipe(breakdown_per_vaccine).to_csv(by_manufacturer_output, index=False)
    base.pipe(global_pipeline).to_csv(global_output, index=False)


if __name__ == "__main__":
    main()
