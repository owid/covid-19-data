import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils


def main():

    url = "https://www.gov.je/Health/Coronavirus/Vaccine/Pages/VaccinationStatistics.aspx"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    table = soup.find(class_="govstyleTable-default")
    df = pd.read_html(str(table))[0]

    total_vaccinations = int(df.loc[df[0] == "Total number of doses", 1].values[0])
    people_vaccinated = int(df.loc[df[0] == "Total number of first dose vaccinations", 1].values[0])
    people_fully_vaccinated = int(df.loc[df[0] == "Total number of second dose vaccinations", 1].values[0])

    date = re.search(r"Data applies to: Week ending (\d[\w\s]+\d{4})", soup.text).group(1)
    date = vaxutils.clean_date(date, "%d %B %Y")

    vaxutils.increment(
        location="Jersey",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
