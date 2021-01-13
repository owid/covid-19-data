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

    count = re.search(r"Număr total de persoane vaccinate împotriva COVID-19 cu vaccinul Pfizer BioNTech \(începând cu data de 27 decembrie 2020\) – ([\d\.]+)", main_text)
    count = count.group(1)
    count = vaxutils.clean_count(count)

    vaxutils.increment(
        location="Romania",
        total_vaccinations=count,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
