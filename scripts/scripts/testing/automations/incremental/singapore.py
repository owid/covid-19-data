import re

from bs4 import BeautifulSoup, element
import pandas as pd

from cowidev.utils.web import get_soup
from cowidev.utils.clean import clean_count, clean_date
from cowidev.testing.utils.incremental import increment


class Singapore:
    location = "Singapore"
    units = "samples tested"
    source_label = "Ministry of Health"
    notes = ""
    _source_url = "https://www.moh.gov.sg/covid-19/statistics"
    _num_max_pages = 3
    regex = {
        "date": r"Swabs Tested (as of (\d+ \w+ \d+)",
        "pcr": "Total PCR Swabs Tested",
        "art": "Total Reportable ART Swabs Tested",
    }

    def read(self) -> pd.Series:
        """Read data from source."""
        soup = get_soup(self._source_url)
        data = self._parse_data(soup)

        return pd.Series(data)

    def _parse_data(self, soup: BeautifulSoup) -> dict:
        """Get data from the source page."""
        # Get relevant elements
        pcr_elem, art_elem = self._get_relevant_elements(soup)
        if not pcr_elem and art_elem:
            raise TypeError("Website Structure Changed, please update the script")
        # Extract date from the element
        date = self._parse_date_from_soup(soup)
        # Extract metrics from the elements
        count = self._parse_metrics(pcr_elem, art_elem)
        record = {
            "source_url": self._source_url,
            "date": date,
            "count": count,
        }
        return record

    def _get_relevant_elements(self, soup: BeautifulSoup) -> tuple:
        """Get the relevant elements in soup."""
        pcr_elem = soup.find(text=self.regex["pcr"])
        art_elem = soup.find(text=self.regex["art"])
        return pcr_elem, art_elem

    def _parse_date_from_soup(self, soup: BeautifulSoup) -> str:
        """Get date from soup."""
        elem_list = soup.find_all("h3")
        date_list = [
            re.search(r"Swabs Tested \(as of (\d+ \w+ \d+)", elem.text).group(1)
            for elem in elem_list
            if re.search(r"Swabs Tested \(as of (\d+ \w+ \d+)", elem.text)
        ]
        if date_list[0] != date_list[1]:
            raise ValueError("pcr and art dates don't match, please update the script")
        date = date_list[0]
        return clean_date(date, "%d %b %Y")

    def _parse_metrics(self, pcr_elem: element.Tag, art_elem: element.Tag) -> int:
        """Get metrics from Tags."""
        pcr_count = int(pcr_elem.find_parent("tr").find_next_sibling("tr").text.replace(",", ""))
        art_count = int(art_elem.find_parent("tr").find_next_sibling("tr").text.replace(",", ""))
        return clean_count(pcr_count + art_count)

    def export(self):
        data = self.read()
        increment(
            sheet_name=self.location,
            country=self.location,
            units=self.units,
            date=data["date"],
            source_url=data["source_url"],
            source_label=self.source_label,
            count=data["count"],
        )


def main():
    Singapore().export()


if __name__ == "__main__":
    main()
