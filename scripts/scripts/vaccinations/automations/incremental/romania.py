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

    regex = r"Număr total de doze administrate de vaccin împotriva COVID-19 Pfizer BioNTech \(începând cu data de 27 decembrie 2020\): ([\d\.]+), la un număr de [a-z]* ?[\d\.]+ [a-z]* ?din care:\n.\s*[\d\.]+\s+persoane vaccinate cu 1 doză;\n.\s*([\d\.]+)\s+persoane vaccinate cu 2 doze"
    
    counts = re.search(regex, main_text)

    total_vaccinations = counts.group(1)
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    people_fully_vaccinated = counts.group(2)
    people_fully_vaccinated = vaxutils.clean_count(people_fully_vaccinated)

    people_vaccinated = total_vaccinations - people_fully_vaccinated

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
