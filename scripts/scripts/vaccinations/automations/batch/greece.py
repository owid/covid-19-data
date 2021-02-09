import json
import requests
import pandas as pd

def main():

    url = "https://data.gov.gr/api/v1/query/mdg_emvolio"

    with open("vax_dataset_config.json", "rb") as file:
        config = json.loads(file.read())

    response = requests.get(url, headers={"Authorization": f"Token {config['greece_api_token']}"})

    df = pd.DataFrame.from_records(response.json())

    df = (
        df.groupby("referencedate", as_index=False)
        [["referencedate", "totalvaccinations", "totaldistinctpersons"]].sum()
    )

    df = df.rename(columns={
        "referencedate": "date",
        "totalvaccinations": "total_vaccinations",
        "totaldistinctpersons": "people_vaccinated",
    })

    df["people_fully_vaccinated"] = (df["total_vaccinations"] - df["people_vaccinated"]).replace(0, pd.NA)

    df.loc[:, "date"] = df["date"].str.slice(0, 10)
    df.loc[:, "location"] = "Greece"
    df.loc[:, "vaccine"] = "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://www.data.gov.gr/datasets/mdg_emvolio/"

    df.to_csv("automations/output/Greece.csv", index=False)

if __name__ == '__main__':
    main()
