import tabula
import pandas as pd
from bs4 import BeautifulSoup
import urllib.request


def main():
    url = "https://covid19.ssi.dk/overvagningsdata/vaccinationstilslutning"

    # Locate newest pdf
    html_page = urllib.request.urlopen(url)
    soup = BeautifulSoup(html_page, "html.parser")
    pdf_path = soup.find('a', text="Download her").get("href")  # Get path to newest pdf

    # Fetch data
    column_string = {'dtype': str , 'header': None}  # Force dtype to be object because of thousand separator in Europe
    kwargs = {'pandas_options': column_string,}
    dfs_from_pdf = tabula.read_pdf(pdf_path, pages="all", **kwargs)
    df = pd.DataFrame(dfs_from_pdf[0])  # Hardcoded table location
    header = (
        df[0:3]
        .astype(str)
        .apply(lambda x: x.replace("nan", ""))
        .apply(' '.join)
        .apply(lambda x: x.strip())
        .replace(r"\s", " ", regex=True)
    )
    df.columns = header
    df = df.drop([0,1,2])
    
    # Manipulate data
    df["date"] = pd.to_datetime(df["Vaccinationsdato"], format="%d-%m-%Y")
    df["people_vaccinated"] = (
        df["Antal personer som har påbegyndt vaccination"]
        .apply(lambda x: x.replace(".", ""))
        .astype(int)
    )
    df["people_fully_vaccinated"] = (
        df["Antal personer som er færdigvaccineret"]
        .apply(lambda x: x.replace(".", "").replace("-", "0"))
        .astype(int)
    )
    df["total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]
    df["people_fully_vaccinated"] = df["people_fully_vaccinated"].replace(0, pd.NA)

    df = df[["date", "people_vaccinated", "people_fully_vaccinated", "total_vaccinations"]]
    
    df.loc[:, "location"] = "Denmark"
    df.loc[:, "source_url"] = pdf_path

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df.date >= "2021-01-13", "vaccine"] = "Moderna, Pfizer/BioNTech"

    df.to_csv("automations/output/Denmark.csv", index=False)


if __name__ == "__main__":
    main()
