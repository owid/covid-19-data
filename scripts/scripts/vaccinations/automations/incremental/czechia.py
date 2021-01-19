import datetime
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import vaxutils

def main():

    url = "https://onemocneni-aktualne.mzcr.cz/covid-19"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    count = soup.find(string=re.compile("Vykázaná očkování")).parent.parent.find(id="count-test").text
    count = vaxutils.clean_count(count)

    date = soup.find(string=re.compile("Uvedený údaj odpovídá kumulativnímu počtu všech vykázaných dávek vakcíny k datu"))
    date = re.search(r"(\d+)\.\s(\d+)\.\s(\d+)", date)
    date = datetime.date(year=int(date.group(3)), month=int(date.group(2)), day=int(date.group(1)))
    date = str(date)

    vaxutils.increment(
        location="Czechia",
        total_vaccinations=count,
        date=date,
        source_url=url,
        vaccine="Moderna, Pfizer/BioNTech"
    )

if __name__ == '__main__':
    main()
