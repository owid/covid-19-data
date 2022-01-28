import re

from bs4 import BeautifulSoup, element
import pandas as pd

from cowidev.utils.web import get_soup
from cowidev.utils.clean import clean_count, clean_date
from cowidev.testing.utils.incremental import increment


class Fiji:
    location = "Fiji"
    units = "tests performed"
    source_label = "Fiji Ministry of Health & Medical Services"
    notes = ""
    source_url = "https://www.health.gov.fj/page/"
    _num_max_pages = 3
    regex = {
        "title": r"COVID-19 Update",
        "year": r"\d{4}",
        "date": r"tests have been reported for (\w+ \d+)",
        "count": r"(\d+) tests have been reported",
    }

    def read(self) -> pd.Series:
        data = []

        for cnt in range(1, self._num_max_pages + 1):
            url = f"{self.source_url}{cnt}/"
            soup = get_soup(url)
            data, proceed = self._parse_data(soup)
            if not proceed:
                break

        return pd.Series(data)

    def _parse_data(self, soup: BeautifulSoup) -> tuple:
        """Get data from the source page."""
        # Get relevant element
        elem, year = self._get_relevant_element_and_year(soup)
        if not elem:
            return None, True
        # Extract url and date from element
        url = self._parse_link_from_element(elem)
        # Extract text from url
        text = self._get_text_from_url(url)
        # Extract metrics from text
        date = self._parse_date_from_text(year, text)
        # Extract metrics from text
        daily_change = self._parse_metrics(text)
        record = {
            "source_url": url,
            "date": date,
            "daily_change": daily_change,
        }
        return record, False

    def _get_relevant_element_and_year(self, soup: BeautifulSoup) -> tuple:
        """Get the relevant element and year from the source page."""
        elem_list = soup.find_all("h2")
        elem = [title for title in elem_list if self.regex["title"] in title.text]
        elem = elem[0]
        if not elem:
            return None, None
        year = re.search(self.regex["year"], elem.text).group()
        return elem, year

    def _parse_date_from_text(self, year: str, text: str) -> str:
        """Get date from relevant element."""
        month_day = re.search(self.regex["date"], text).group(1)
        return clean_date(f"{month_day} {year}", "%B %d %Y")

    def _parse_link_from_element(self, elem: element.Tag) -> str:
        """Get link from relevant element."""
        link = elem.find("a")["href"]
        return link

    def _get_text_from_url(self, url: str) -> str:
        """Extract text from the url."""
        soup = get_soup(url)
        text = soup.get_text(strip=True).replace("\n", " ").lower()
        return text

    def _parse_metrics(self, text: str) -> int:
        """Get metrics from news text."""
        count = re.search(self.regex["count"], text).group(1)
        return clean_count(count)

    def export(self):
        data = self.read()
        increment(
            sheet_name=self.location,
            country=self.location,
            units=self.units,
            date=data["date"],
            source_url=data["source_url"],
            source_label=self.source_label,
            daily_change=data["daily_change"],
        )


def main():
    Fiji().export()


if __name__ == "__main__":
    main()
