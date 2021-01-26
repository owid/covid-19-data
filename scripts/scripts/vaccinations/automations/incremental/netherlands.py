import re
import requests
from bs4 import BeautifulSoup
import dateparser
import pandas as pd
import vaxutils


def main():

    url = "https://github.com/mzelst/covid-19/raw/master/data/all_data.csv"
    df = pd.read_csv(url, usecols=["date", "vaccines_administered"])
    df = df.dropna().sort_values("date").tail(1)

    total_vaccinations = int(df["vaccines_administered"].values[0])
    date = df["date"].values[0]

    vaxutils.increment(
        location="Netherlands",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url="https://coronadashboard.rijksoverheid.nl/landelijk/vaccinaties",
        vaccine="Moderna, Pfizer/BioNTech"
    )


if __name__ == '__main__':
    main()
