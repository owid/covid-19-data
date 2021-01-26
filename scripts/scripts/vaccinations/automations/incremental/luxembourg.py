import requests
import tabula
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils


def main():
    url = "https://data.public.lu/fr/datasets/covid-19-rapports-journaliers/#_"

    # Locate newest pdf
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    pdf_path = soup.find("a", class_="btn-primary")["href"]  # Get path to newest pdf

    # Fetch data
    dfs_from_pdf = tabula.read_pdf(pdf_path, pages="all")
    df = pd.DataFrame(dfs_from_pdf[2])  # Hardcoded table location

    people_vaccinated = df.loc[df["Unnamed: 0"] == "Personnes vaccinées - Dose 1", "Unnamed: 2"].values[0]
    people_vaccinated = vaxutils.clean_count(people_vaccinated)

    people_fully_vaccinated = df.loc[df["Unnamed: 0"] == "Personnes vaccinées - Dose 2", "Unnamed: 2"].values[0]
    people_fully_vaccinated = vaxutils.clean_count(people_fully_vaccinated)

    total_vaccinations = people_vaccinated + people_fully_vaccinated
    
    date = df["Unnamed: 1"].str.replace("Journée du ", "").values[0]
    date = vaxutils.clean_date(date, "%d.%m.%Y")

    vaxutils.increment(
        location="Luxembourg",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=pdf_path,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
