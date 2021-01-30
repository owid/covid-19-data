import requests
from bs4 import BeautifulSoup
import pandas as pd
import vaxutils

def main():

    url = "https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/vacunaCovid19.htm"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    for a in soup.find(class_="menuCCAES").find_all("a"):
        if ".ods" in a["href"]:
            url = "https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/" + a["href"]

    df = pd.read_excel(url)

    total_vaccinations = int(df.loc[df["Unnamed: 0"] == "Totales", "Dosis administradas (2)"].values[0])
    people_fully_vaccinated = int(df.loc[df["Unnamed: 0"] == "Totales", "Nº Personas vacunadas(pauta completada)"].values[0])
    people_vaccinated = total_vaccinations - people_fully_vaccinated

    date = str(df["Fecha de la última vacuna registrada (2)"].max().date())

    vaxutils.increment(
        location="Spain",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=url,
        vaccine="Moderna, Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
