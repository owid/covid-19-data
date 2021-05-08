import os
import requests
import tempfile

import pandas as pd
from bs4 import BeautifulSoup


def read_datasets_download_link(source: str) -> str:
    page = requests.get(source).content
    soup = BeautifulSoup(page, "html.parser")
    for elem in soup.find_all("a", class_="footer__nav__link"):
        if "sources-csv" in elem.get("href"):
            return "https://www.covid19.admin.ch" + elem.get("href")
    raise Exception("No CSV link found in footer.")


def read_vaccination_datasets(source: str):
    csv_url = read_datasets_download_link(source)
    with tempfile.TemporaryDirectory(dir=".") as temp_dir:
        temp_file = f"{temp_dir}/data.zip"
        os.system(f"curl {csv_url} -o {temp_file} -s > /dev/null")
        os.system(f"unzip -d {temp_dir} {temp_file} > /dev/null")
        doses = pd.read_csv(
            os.path.join(temp_dir, "data/COVID19VaccDosesAdministered.csv"),
            usecols=["geoRegion", "date", "sumTotal", "type"]
        )
        people = pd.read_csv(
            os.path.join(temp_dir, "data/COVID19FullyVaccPersons.csv"),
            usecols=["geoRegion", "date", "sumTotal", "type"]
        )
        return pd.concat([doses, people], ignore_index=True)


def filter_country(df: pd.DataFrame, country_code: str) -> pd.DataFrame:
    return df[df.geoRegion == country_code]


def pivot(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pivot(index=["geoRegion", "date"], columns="type", values="sumTotal")
        .reset_index()
        .sort_values("date")
    )


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={
        "geoRegion": "location",
        "COVID19FullyVaccPersons": "people_fully_vaccinated",
        "COVID19VaccDosesAdministered": "total_vaccinations",
    })


def translate_country_code(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        location=df.location.replace({"CH": "Switzerland", "FL": "Liechtenstein"})
    )


def enrich(df: pd.DataFrame, country_code: str) -> pd.DataFrame:
    return df.assign(
        people_vaccinated=df.total_vaccinations - df.people_fully_vaccinated,
        source_url=f"https://www.covid19.admin.ch/en/epidemiologic/vacc-doses?detGeo={country_code}",
    )


def enrich_vaccine(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine(date: str) -> str:
        if date >= "2021-01-29":
            return "Moderna, Pfizer/BioNTech"
        return "Pfizer/BioNTech"

    return df.assign(vaccine=df.date.astype(str).apply(_enrich_vaccine))


def pipeline(df: pd.DataFrame, country_code: str) -> pd.DataFrame:
    return (
        df
        .pipe(filter_country, country_code)
        .pipe(pivot)
        .pipe(rename_columns)
        .pipe(translate_country_code)
        .pipe(enrich, country_code)
        .pipe(enrich_vaccine)
    )


def main(paths):
    source = "https://www.covid19.admin.ch/en/epidemiologic/vacc-doses"
    data = read_vaccination_datasets(source)

    data.pipe(pipeline, country_code="CH").to_csv(
        paths.out_tmp("Switzerland"),
        index=False
    )
    data.pipe(pipeline, country_code="FL").to_csv(
        paths.out_tmp("Liechtenstein"),
        index=False
    )

if __name__ == "__main__":
    main()
