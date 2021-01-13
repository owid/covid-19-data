from datetime import datetime
import pytz
import pandas as pd
import requests


def get_vaccine_name(df):
    """Return vaccine names.

    Add logic here if different vaccines were in use in different periods.
 
    Args:
        df (pandas.DataFrame): Data. Must contain column named 'date' with date information.

    Returns:
        str or array: String if value is constant or array if different values are present.
    """
    return "Pfizer/BioNTech"


def main():
    """Main function."""
    # Define URL of potential last release file
    date_str = datetime.now(pytz.timezone("America/Santiago")).date().strftime("%Y-%m-%d")
    url = f"https://github.com/juancri/covid19-vaccination/releases/download/{date_str}/output.csv"

    # Verify release file exists
    status_code = requests.get(url).status_code
    if status_code == 200:
        # Load data
        df = pd.read_csv(url)

        # Build dataframe
        df = df.drop(columns=('country')).cumsum(axis=1).T.reset_index()
        df = df.rename(columns={
            "index": "date",
            0: "total_vaccinations"
        })
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df.loc[:, "location"] = "Chile"
        df.loc[:, "vaccine"] = get_vaccine_name(df)
        df.loc[:, "source_url"] = "https://github.com/juancri/covid19-vaccination/releases/"

        # Save
        df.to_csv("automations/output/Chile.csv", index=False)


if __name__ == "__main__":
    main()
