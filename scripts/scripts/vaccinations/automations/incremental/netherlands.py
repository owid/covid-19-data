import dateparser
from bs4 import BeautifulSoup
import requests
import vaxutils


def main():

    baseurl = 'https://coronadashboard.rijksoverheid.nl'

    page = requests.get(baseurl + '/landelijk/vaccinaties')
    soup = BeautifulSoup(page.text, "html.parser")

    for p in soup.find_all('p'):
        if 'Laatste waardes verkregen op' in p.text:
            date = p.text.split('.')[0].split('op')[1]
            date = str(dateparser.parse(date, languages=["nl"]).date())

    for article in soup.find_all('article'):
        for h3 in article.find_all('h3'):
            text = h3.text.strip()
            if 'Aantal toegediende vaccins' in text:
                total_vaccinations = int(article.select('[class*="kpi-value_"]')[0].text.replace('.', ''))

    vaxutils.increment(
        location="Netherlands",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url="https://coronadashboard.rijksoverheid.nl/landelijk/vaccinaties",
        vaccine="Moderna, Pfizer/BioNTech"
    )


if __name__ == '__main__':
    main()
