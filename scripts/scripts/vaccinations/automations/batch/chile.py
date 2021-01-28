import pandas as pd


def get_vaccine_name(df):
    """Return vaccine names.

    Add logic here if different vaccines were in use in different periods.
 
    Args:
        df (pandas.DataFrame): Data. Must contain column named 'date' with date information.

    Returns:
        str or array: String if value is constant or array if different values are present.
    """
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    return df


def main():
    # Load data
    url = "https://github.com/juancri/covid19-vaccination/raw/master/output/chile-vaccination.csv"
    df = pd.read_csv(url)

    # Process data
    df = df.loc[df["Region"] == "Total"].T.drop(["Region", "Dose"]).astype(int)
    df = df.reset_index().rename(columns={
        "index": "date",
        0: "people_vaccinated",
        1: "people_fully_vaccinated"
    })
    df = df.groupby(
        by=["people_vaccinated", "people_fully_vaccinated"]
    ).min().reset_index()

    # Add columns
    df.loc[:, "total_vaccinations"] = df.loc[:, "people_vaccinated"] + df.loc[:, "people_fully_vaccinated"]
    df["people_fully_vaccinated"] = df["people_fully_vaccinated"].replace({0: pd.NA})
    df.loc[:, "location"] = "Chile"
    df = get_vaccine_name(df)
    df.loc[:, "source_url"] = "https://informesdeis.minsal.cl/SASVisualAnalytics/?reportUri=%2Freports%2Freports%2F1a8cc7ff-7df0-474f-a147-929ee45d1900&sectionIndex=0&sso_guest=true&reportViewOnly=true&reportContextBar=false&sas-welcome=false"

    # Save
    df = df[["location", "date", "people_vaccinated", "people_fully_vaccinated", "total_vaccinations", 
             "vaccine", "source_url"]]
    df.to_csv("automations/output/Chile.csv", index=False)


if __name__ == "__main__":
    main()
