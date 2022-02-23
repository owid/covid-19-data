import re

from cowidev.utils.web import get_soup
from cowidev.testing.utils.incremental import increment
from cowidev.utils.clean import clean_count, clean_date


class Lebanon:
    location = "Lebanon"
    units = "tests performed"
    source_label = "Lebanon Ministry of Health"
    source_url = "https://corona.ministryinfo.gov.lb/"
    notes = ""
    regex = {"date": r"([A-Za-z]+ \d+)"}

    def read(self):
        soup = get_soup(self.source_url)

        count = self._parse_count(soup)
        date = self._parse_date(soup)

        return {"count": count, "date": date}

    def _parse_count(self, soup: str) -> str:
        count = soup.find("h1", class_="s-counter3").text

        return clean_count(count)

    def _parse_date(self, soup: str) -> str:
        date = soup.select(".last-update strong")[0].text
        date = re.search(self.regex["date"], date).group(0)

        return clean_date(f"2021{date}", "%Y%b %d")

    def export(self):
        data = self.read()
        increment(
            count=data["count"],
            sheet_name=self.location,
            country=self.location,
            units=self.units,
            date=data["date"],
            source_url=self.source_url,
            source_label=self.source_label,
        )


def main():
    Lebanon().export()
