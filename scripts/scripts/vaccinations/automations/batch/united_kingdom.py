import pandas as pd

def get_metrics(metrics, area):
    metrics = "".join(f"&metric={m}" for m in metrics)
    url = f"https://api.coronavirus.data.gov.uk/v2/data?areaType={area}&format=csv{metrics}"
    return pd.read_csv(url)

def main():

    metrics = [
        "cumPeopleVaccinatedFirstDoseByPublishDate",
        "cumPeopleVaccinatedFirstDoseByVaccinationDate",
        "cumPeopleVaccinatedSecondDoseByPublishDate",
        "cumPeopleVaccinatedSecondDoseByVaccinationDate",
    ]

    uk = get_metrics(metrics, "overview")
    subnational = get_metrics(metrics, "nation")

    df = pd.concat([uk, subnational]).reset_index(drop=True)
    df = df.rename(columns={"areaName": "location"})

    df["dose1"] = df.cumPeopleVaccinatedFirstDoseByPublishDate
    df.loc[df["dose1"].isna(), "dose1"] = df.cumPeopleVaccinatedFirstDoseByVaccinationDate

    df["dose2"] = df.cumPeopleVaccinatedSecondDoseByPublishDate
    df.loc[df["dose2"].isna(), "dose2"] = df.cumPeopleVaccinatedSecondDoseByVaccinationDate

    df["total_vaccinations"] = (df["dose1"].add(df["dose2"]))

    df["people_vaccinated"] = df["dose1"]

    df["people_fully_vaccinated"] = df["dose2"]

    df = (
        df
        .groupby(["location", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated"], as_index=False)
        [["date"]].min()
        .replace(0, pd.NA)
    )

    df.loc[:, "source_url"] = "https://coronavirus.data.gov.uk/details/healthcare"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df["date"] >= "2021-01-04", "vaccine"] = "Oxford/AstraZeneca, Pfizer/BioNTech"

    for loc in set(df["location"]):
        df[df["location"] == loc].to_csv(f"automations/output/{loc}.csv", index=False)

if __name__ == "__main__":
    main()
