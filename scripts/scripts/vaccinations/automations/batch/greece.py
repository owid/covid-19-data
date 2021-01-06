import json
import requests
import pandas as pd

def main():

    url = "https://data.gov.gr/api/v1/query/mdg_emvolio?date_from=2020-12-29&date_to=2021-01-05"

    with open("vax_dataset_config.json", "rb") as file:
        config = json.loads(file.read())

    response = requests.get(url, headers={"Authorization": f"Token {config['greece_api_token']}"})

    df = pd.DataFrame.from_records(response.json())

    df = df.groupby("referencedate", as_index=False)[["referencedate", "totalvaccinations"]].sum()

    df = df.rename(columns={"referencedate": "date", "totalvaccinations": "total_vaccinations"})

    df.loc[:, "date"] = df["date"].str.slice(0, 10)
    df.loc[:, "location"] = "Greece"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://www.data.gov.gr/datasets/mdg_emvolio/"

    df.to_csv("automations/output/Greece.csv", index=False)

if __name__ == '__main__':
    main()
