import os
import json

import requests
from bs4 import BeautifulSoup
import pandas as pd

from vax.utils.incremental import increment


VACCINE_PROTOCOLS = {
    "Pfizer": 2,
    "Moderna": 2,
    "AstraZeneca": 2,
    "Janssen": 1,
}


def main(paths):

    url = "https://e.infogram.com/c3bc3569-c86d-48a7-9d4c-377928f102bf"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    for script in soup.find_all("script"):
        if "infographicData" in str(script):
            json_data = (
                str(script)
                .replace("<script>window.infographicData=", "")
                .replace(";</script>", "")
            )
            json_data = json.loads(json_data)
            break

    data = (
        json_data["elements"]["content"]["content"]["entities"]["39ac25a9-8af7-4d26-bd19-62a3696920a2"]["props"]
        ["chartData"]["data"][0]
    )

    df = pd.DataFrame(data[1:], columns=data[0])

    assert set(df.iloc[:, 0]) == set(VACCINE_PROTOCOLS.keys()), "New vaccine found!"
    
    total_vaccinations = 0
    people_vaccinated = 0
    people_fully_vaccinated = 0

    for row in df.iterrows():
        protocol = VACCINE_PROTOCOLS[row[1][0]]

        if protocol == 1:
            total_vaccinations += row[1]["Fullbólusettir"]
            people_vaccinated += row[1]["Fullbólusettir"]
            people_fully_vaccinated += row[1]["Fullbólusettir"]

        elif protocol == 2:
            total_vaccinations += row[1]["Fullbólusettir"] * 2 + row[1]["Bólusetning hafin"]
            people_vaccinated += row[1]["Fullbólusettir"] + row[1]["Bólusetning hafin"]
            people_fully_vaccinated += row[1]["Fullbólusettir"]

    date = json_data["updatedAt"][:10]

    increment(
        paths=paths,
        location="Iceland",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="https://www.covid.is/tolulegar-upplysingar-boluefni",
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    )

    # By manufacturer
    data = (
        json_data["elements"]["content"]["content"]["entities"]["e329559c-c3cc-48e9-8b7b-1a5f87ea7ad3"]["props"]
        ["chartData"]["data"][0]
    )
    df = pd.DataFrame(data[1:]).reset_index(drop=True)
    df.columns = ["date"] + data[0][1:]

    df = df.melt("date", var_name="vaccine", value_name="total_vaccinations")

    df["date"] = pd.to_datetime(df["date"], format="%d.%m.%y")
    df["total_vaccinations"] = pd.to_numeric(df["total_vaccinations"], errors="coerce").fillna(0)
    df["total_vaccinations"] = df.sort_values("date").groupby("vaccine", as_index=False)["total_vaccinations"].cumsum()
    df["location"] = "Iceland"

    vaccine_mapping = {
        "Pfizer/BioNTech": "Pfizer/BioNTech",
        "Moderna": "Moderna",
        "Oxford/AstraZeneca": "Oxford/AstraZeneca",
        "Janssen": "Johnson&Johnson",
    }
    assert set(df["vaccine"].unique()) == set(vaccine_mapping.keys()), \
        f"Vaccines present in data: {df['vaccine'].unique()}"
    df = df.replace(vaccine_mapping)

    df.to_csv(paths.tmp_vax_loc_man("Iceland"), index=False)


if __name__ == '__main__':
    main()
