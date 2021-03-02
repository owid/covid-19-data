import requests

import pandas as pd


def read(source: str) -> pd.DataFrame:
    data = requests.get(source).json()
    return (
        pd.DataFrame.from_dict(data["historicalData"], orient="index")
        .reset_index()
        [["index", "vaccines", "numberTotalDosesAdministered"]]
        .rename(columns={"index": "date", "numberTotalDosesAdministered": "total_vaccinations"})
        .dropna()
        .sort_values("date")
        .assign(location="Romania")
    )


def parse_nested_json(row, metric: str) -> int:
    values = [v[metric] for k,v in row["vaccines"].items()]
    return sum(values)


def process_vaccine_data(df: pd.DataFrame) -> pd.DataFrame:

    df["people_fully_vaccinated"] = df.apply(parse_nested_json, metric="immunized", axis=1).cumsum()
    df["people_vaccinated"] = df.total_vaccinations - df.people_fully_vaccinated

    assert set(df.iloc[-1].vaccines.keys()) == set(('pfizer', 'astra_zeneca', 'moderna'))
    df["vaccine"] = "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

    df = df.drop(columns="vaccines")

    df["source_url"] = "https://datelazi.ro/"

    return df


def parse_vaccine_doses(row):
    new_row = pd.Series({k:v["total_administered"] for k,v in row["vaccines"].items()})
    new_row["date"] = row["date"]
    new_row["location"] = row["location"]
    return new_row


def process_manufacturer_data(df: pd.DataFrame) -> pd.DataFrame:

    df = df.apply(parse_vaccine_doses, axis=1)

    df = df.melt(id_vars=["date", "location"], var_name="vaccine", value_name="total_vaccinations")

    vaccine_mapping = {
        "pfizer": "Pfizer/BioNTech",
        "moderna": "Moderna",
        "astra_zeneca": "Oxford/AstraZeneca",
    }
    assert set(df["vaccine"].unique()) == set(vaccine_mapping.keys())
    df = df.replace(vaccine_mapping)

    df["total_vaccinations"] = df.groupby("vaccine", as_index=False).cumsum()

    return df


if __name__ == "__main__":

    source = "https://d35p9e4fm9h3wo.cloudfront.net/latestData.json"
    destination = "automations/output/Romania.csv"

    data = read(source)

    process_vaccine_data(data.copy()).to_csv(destination, index=False)
    process_manufacturer_data(data.copy()).to_csv(destination.replace("output", "output/by_manufacturer"), index=False)
