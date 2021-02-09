import requests
from bs4 import BeautifulSoup
import pandas as pd
import tabula
import vaxutils


def main():

    url = "https://vaccinare-covid.gov.ro/comunicate-oficiale/"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    links = soup.find(class_="display-posts-listing").find_all("a", class_="title")

    for link in links:
        if "Actualizare zilnică" in link.text:
            url = link["href"]
            break

    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    date = soup.find(class_="post-date").find(class_="meta-text").text.strip()
    date = vaxutils.clean_date(date, "%b %d, %Y")

    url = soup.find(class_="entry-content-text").find_all("a")[-1]["href"]

    kwargs = {'pandas_options': {'dtype': str , 'header': None}}
    dfs_from_pdf = tabula.read_pdf(url, pages="all", **kwargs)
    df = dfs_from_pdf[0]

    values = df[df[0] == "Total"].dropna()[2].str.split(" ")
    values = [vaxutils.clean_count(val) for val in pd.core.common.flatten(values)]
    assert len(values) == 2

    people_fully_vaccinated = min(values)
    people_vaccinated = sum(values)
    total_vaccinations = people_fully_vaccinated + people_vaccinated

    vaxutils.increment(
        location="Romania",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=url,
        vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
