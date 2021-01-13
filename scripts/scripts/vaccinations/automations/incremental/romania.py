import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
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
    
    paragraphs = soup.find(class_="entry-content").find_all("li")

    for paragraph in paragraphs:
        if "Număr total de persoane vaccinate" in paragraph.text:
            count = paragraph.text
            break

    count = re.search(r"[\d\.]+$", count).group(0)
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
