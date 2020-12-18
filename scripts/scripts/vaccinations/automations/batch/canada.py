import json
import requests
import pandas as pd

def main():

    url = "https://api.covid19tracker.ca/reports?stat=vaccinations&after=2020-12-14"
    data = requests.get(url).json()
    data = json.dumps(data["data"])

    df = pd.read_json(data, orient="records")
    df = df.drop(columns=["change_vaccinations"])

    df.loc[:, "location"] = "Canada"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[:, "source_url"] = url

    df.to_csv("automations/output/Canada.csv", index=False)

if __name__ == "__main__":
    main()
