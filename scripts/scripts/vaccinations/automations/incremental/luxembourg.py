import re
import urllib.request
import tabula
import pandas as pd
from bs4 import BeautifulSoup
import vaxutils


def main():
    url = "https://data.public.lu/fr/datasets/covid-19-rapports-journaliers/#_"

    # Locate newest pdf
    html_page = urllib.request.urlopen(url)
    soup = BeautifulSoup(html_page, "html.parser")
    pdf_path = soup.find("a", class_="btn-primary")["href"]  # Get path to newest pdf

    # Fetch data
    dfs_from_pdf = tabula.read_pdf(pdf_path, pages="all")
    df = pd.DataFrame(dfs_from_pdf[2])  # Hardcoded table location
    
    total_vaccinations = df.loc[df["Unnamed: 0"] == "Nombre de doses administrées", "Unnamed: 1"].values[0]
    total_vaccinations = vaxutils.clean_count(total_vaccinations)
    
    date = re.search(r"\d\d\.\d\d\.202\d", df.columns[1]).group(0)
    date = vaxutils.clean_date(date, "%d.%m.%Y")

    vaxutils.increment(
        location="Luxembourg",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url=pdf_path,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
