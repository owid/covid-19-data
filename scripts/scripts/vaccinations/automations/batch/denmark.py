import tabula
import pandas as pd
from bs4 import BeautifulSoup
import urllib.request


def main():
    url = "https://covid19.ssi.dk/overvagningsdata/vaccinationstilslutning"

    # Locate newest pdf
    html_page = urllib.request.urlopen(url)
    soup = BeautifulSoup(html_page, "html.parser")
    pdf_path = soup.find('a', text = "Download her").get("href")  # Get path to newest pdf

    # Fetch data
    df = pd.DataFrame()
    column_string = {'dtype': str}  # Force dtype to be object because of thousand separator in Europe
    kwargs = {'pandas_options': column_string}
    dfs_from_pdf = tabula.read_pdf(pdf_path, pages = "all", **kwargs)
    vaccinated_df = dfs_from_pdf[1]  # Hardcoded table location

    # Manipulate data
    df["date"] = pd.to_datetime(vaccinated_df["Vaccinationsdato"], format = "%d-%m-%Y")
    vaccinated_df.columns = vaccinated_df.columns.str.replace(r"\s", " ", regex=True)
    df["total_vaccinations"] = vaccinated_df["Antal personer som har påbegyndt covid-19 vaccination"].apply(lambda x: x.replace(".", "")).astype(int)
    df = df.groupby("total_vaccinations", as_index = False).min()
    
    df.loc[:, "location"] = "Denmark"
    df.loc[:, "source_url"] = pdf_path

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df.date >= "2021-01-13", "vaccine"] = "Moderna, Pfizer/BioNTech"

    df.to_csv("automations/output/Denmark.csv", index=False)


if __name__ == "__main__":
    main()
