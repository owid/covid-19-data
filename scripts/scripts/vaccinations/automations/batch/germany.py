from typing import Dict

import pandas as pd


TRANSLATIONS = {
    "dosen_kumulativ": "total_vaccinations",
    "personen_erst_kumulativ": "people_vaccinated",
    "personen_voll_kumulativ": "people_fully_vaccinated",
    "dosen_biontech_kumulativ": "Pfizer/BioNTech",
    "dosen_moderna_kumulativ": "Moderna",
    "dosen_astrazeneca_kumulativ": "Oxford/AstraZeneca",
}


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(
        source,
        sep="\t",
        usecols=[
            "date",
            "dosen_kumulativ",
            "personen_erst_kumulativ",
            "personen_voll_kumulativ",
            "dosen_biontech_kumulativ",
            "dosen_moderna_kumulativ",
            "dosen_astrazeneca_kumulativ",
        ],
    )


def translate_columns(
    input: pd.DataFrame, translations: Dict[str, str]
) -> pd.DataFrame:
    return input.rename(columns=translations)


def enrich_location(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(location="Germany")


def melt_manufacturers(input: pd.DataFrame) -> pd.DataFrame:
    return input[["date", "Pfizer/BioNTech", "Moderna", "Oxford/AstraZeneca"]].melt(
        "date", var_name="vaccine", value_name="total_vaccinations"
    )


def by_manufacturer_pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return input.pipe(melt_manufacturers).pipe(enrich_location)


def base_pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return input.pipe(translate_columns, TRANSLATIONS)


def enrich_vaccine(input: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine(date: str) -> str:
        if date >= "2021-02-08":
            return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        if date >= "2021-01-12":
            return "Moderna, Pfizer/BioNTech"
        return "Pfizer/BioNTech"

    return input.assign(vaccine=input.date.apply(_enrich_vaccine))


def enrich_metadata(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        location="Germany",
        source_url="https://impfdashboard.de/",
    )


def select_output_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input[
        ["date", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]
    ]


def overall_pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return input.pipe(select_output_columns).pipe(enrich_metadata).pipe(enrich_vaccine)


def main():
    source = (
        "https://impfdashboard.de/static/data/germany_vaccinations_timeseries_v2.tsv"
    )
    by_manufacturer_destination = "automations/output/by_manufacturer/Germany.csv"
    destination = "automations/output/Germany.csv"

    base = read(source).pipe(base_pipeline)
    base.pipe(by_manufacturer_pipeline).to_csv(by_manufacturer_destination, index=False)
    base.pipe(overall_pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
