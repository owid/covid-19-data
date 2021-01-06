import datetime
import requests
from bs4 import BeautifulSoup
import vaxutils

def main():

    url = "https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/vacunaCovid19.htm"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    count = soup.find(class_="banner-vacunas").find(class_="cifra").text
    count = vaxutils.clean_count(count)

    vaxutils.increment(
        location="Spain",
        total_vaccinations=count,
        date=str(datetime.date.today()),
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
