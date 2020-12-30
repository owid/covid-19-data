import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import vax_utils

def main():
     
    page = requests.get("https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquoten-Tab.html")
    soup = BeautifulSoup(page.content, "html.parser")

    text = soup.find(id="main").find(class_="box").find("strong").text

    regex = re.compile(r"Gesamtzahl der Impfungen bis einschl\. (\d\d\.\d\d\.202\d): ([\d\.]+)")

    date = regex.match(text).group(1)
    date = str(pd.to_datetime(date, format="%d.%m.%Y").date())

    count = regex.match(text).group(2)
    count = int(count.replace(".", ""))

    vax_utils.increment(
        location="Germany",
        total_vaccinations=count,
        date=date,
        source_url="https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquoten-Tab.html",
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
