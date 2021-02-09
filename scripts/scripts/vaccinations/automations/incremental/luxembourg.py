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

    values = sorted(pd.to_numeric(df["Unnamed: 2"].str.replace(r"[^\d]", "", regex=True)).dropna().astype(int))
    assert len(values) == 3

    total_vaccinations = values[2]
    people_vaccinated = values[1]
    people_fully_vaccinated = values[0]
    
    date = df["Unnamed: 1"].str.replace("Journée du ", "").values[0]
    date = vaxutils.clean_date(date, "%d.%m.%Y")

    vaxutils.increment(
        location="Luxembourg",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=pdf_path,
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
