import re
import requests
from bs4 import BeautifulSoup
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
    
    main_text = soup.find(class_="entry-content-text").text

    counts = re.search(r"Număr total de persoane vaccinate împotriva COVID-19 cu vaccinul Pfizer BioNTech \(începând cu data de 27 decembrie 2020\): ([\d\.]+) persoane vaccinate, din care ([\d\.]+) persoane vaccinate cu rapel;", main_text)

    people_vaccinated = counts.group(1)
    people_vaccinated = vaxutils.clean_count(people_vaccinated)

    people_fully_vaccinated = counts.group(2)
    people_fully_vaccinated = vaxutils.clean_count(people_fully_vaccinated)

    total_vaccinations = people_vaccinated + people_fully_vaccinated

    vaxutils.increment(
        location="Romania",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
