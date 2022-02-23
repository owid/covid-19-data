import re

from cowidev.utils.web import get_soup
from cowidev.testing.utils.incremental import increment
from cowidev.utils.clean import clean_count, clean_date


class PapuaNewGuinea:
    location = "Papua New Guinea"
    units = "people tested"
    source_label = "Ministry of Health"
    source_url = "https://covid19.info.gov.pg/"
    source_url_ref = "https://covid19.info.gov.pg/"
    notes = ""
    regex = {
        "date": r"\d{1,2}[a-z]{2} [A-Za-z]+ \d{4}",
    }

    def read(self):
        soup = get_soup(self.source_url)

        count = self._parse_count(soup)
        date = self._parse_date(soup)

        return {"count": count, "date": date}

    def _parse_count(self, soup: str) -> str:
        count = soup.select(".elementor-element-2a79a17 strong")[0].text.replace(" ", "")

        return clean_count(count)

    def _parse_date(self, soup: str) -> str:
        date = soup.select(".elementor-element-00a2010 span")[0].text
        date = re.search(self.regex["date"], date).group(0)
        date = re.sub(r"(?<=\d)[a-z]{2}", "", date)

        return clean_date(date, "%d %B %Y")

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
    PapuaNewGuinea().export()
