import re

from bs4 import BeautifulSoup, element
import pandas as pd

from cowidev.utils.web import get_soup
from cowidev.utils.clean import clean_count, clean_date
from cowidev.testing.utils.incremental import increment


import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Albania:
    location = "Albania"
    units = "tests performed"
    source_label = "Ministry of Health and Social Protection"
    notes = ""
    _base_url = "https://shendetesia.gov.al"
    _url_subdirectory = "category/lajme/page"
    _num_max_pages = 3
    regex = {
        "title": r"COVID19/ Ministria e Shëndetësisë:",
        "date": r"(\d+\/\d+\/\d+)",
        "count": r"(\d+) testime nga të cilat kanë rezultuar pozitivë me COVID19",
    }

    def read(self) -> pd.Series:
        data = []

        for cnt in range(1, self._num_max_pages + 1):
            url = f"{self._base_url}/{self._url_subdirectory}/{cnt}/"
            soup = get_soup(url, verify=False)
            data, proceed = self._parse_data(soup)
            if not proceed:
                break

        return pd.Series(data)

    def _parse_data(self, soup: BeautifulSoup) -> tuple:
        """Get data from the source page."""
        # Get relevant element
        elem = self._get_relevant_element(soup)
        if not elem:
            return None, True
        # Extract url and date from element
        url, date = self._get_link_and_date_from_element(elem)
        # Extract text from url
        text = self._get_text_from_url(url)
        daily_change = self._parse_metrics(text)
        record = {
            "source_url": url,
            "date": date,
            "daily_change": daily_change,
        }
        return record, False

    def _get_relevant_element(self, soup: BeautifulSoup) -> element.Tag:
        """Get the relevant element in news feed."""
        news_list = soup.find_all("h2")

        url_idx = [
            i for i, news in enumerate(news_list) if re.search(self.regex["title"], news.text)
        ]

        if not url_idx:
            return None
        return news_list[url_idx[0]]

    def _get_text_from_url(self, url: str) -> str:
        """Extract text from the url."""
        soup = get_soup(url, verify=False)
        text = (
            soup.find("div", class_="pageDescription")
            .get_text(strip=True)
            .replace(",", "")
        )
        return text

    def _get_link_and_date_from_element(self, elem: element.Tag) -> tuple:
        """Extract link and date from relevant element."""
        link = self._parse_link_from_element(elem)
        if not link:
            return None
        date = self._parse_date_from_element(elem)
        return link, date

    def _parse_date_from_element(self, elem: element.Tag) -> str:
        """Get date from relevant element."""
        date_tag = elem.findNextSibling("div", class_="dateDetalils")
        date = re.search(self.regex["date"], date_tag.text).group()
        return clean_date(date, "%d/%m/%Y")

    def _parse_link_from_element(self, elem: element.Tag) -> str:
        """Get link from relevant element."""
        href = elem.findChild("a")["href"]
        link = f"{href}"
        return link

    def _parse_metrics(self, text: str) -> int:
        """Get metrics from news text."""
        count = int(re.search(self.regex["count"], text).group(1))
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
    Albania().export()


if __name__ == "__main__":
    main()
