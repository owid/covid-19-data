import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
import vaxutils


def main():

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

    data = json_data["elements"]["content"]["content"]["entities"]["39ac25a9-8af7-4d26-bd19-62a3696920a2"]["props"]["chartData"]["data"][0]

    df = pd.DataFrame(data[1:], columns=data[0])

    only_1dose_people = int(df["Bólusetning hafin"].astype(int).sum())
    people_fully_vaccinated = int(df["Fullbólusettir"].astype(int).sum())
    people_vaccinated = only_1dose_people + people_fully_vaccinated
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    date = json_data["updatedAt"][:10]

    vaxutils.increment(
        location="Iceland",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="https://www.covid.is/tolulegar-upplysingar-boluefni",
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    )

    # By manufacturer
    data = json_data["elements"]["content"]["content"]["entities"]["e329559c-c3cc-48e9-8b7b-1a5f87ea7ad3"]["props"]["chartData"]["data"][0]
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
    }
    assert set(df["vaccine"].unique()) == set(vaccine_mapping.keys()), \
        f"Vaccines present in data: {df['vaccine'].unique()}"
    df = df.replace(vaccine_mapping)

    df.to_csv("automations/output/by_manufacturer/Iceland.csv", index=False)


if __name__ == '__main__':
    main()
