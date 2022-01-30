import re
import tempfile

from bs4 import BeautifulSoup, element
import pandas as pd
import pytesseract
from pdf2image import convert_from_path

from cowidev.utils.web import get_soup
from cowidev.utils.web.download import download_file_from_url
from cowidev.utils.clean import clean_count, clean_date
from cowidev.testing.utils.incremental import increment


class DominicanRepublic:
    location = "Dominican Republic"
    units = "samples tested"
    source_label = "Ministry of Public Health and Social Assistance"
    source_url = "https://coronavirusrd.gob.do/documentos/boletines/"
    regex = {
        "date": r"(\d{1,2}\/\d{1,2}\/\d{4})",
        "count": r"24 horas (\d+) \|",
    }

    def read(self) -> pd.Series:
        """Read data from source."""
        soup = get_soup(self.source_url)
        data = self._parse_data(soup)
        return pd.Series(data)

    def _parse_data(self, soup: BeautifulSoup) -> dict:
        """Get data from the source page."""
        # Get relevant element
        elem = self._get_relevant_element(soup)
        # Extract pdf link from url
        pdf_url = self._get_pdf_link_from_element(elem)
        # Get text from pdf link
        text = self._parse_pdf_link(pdf_url)
        print(text)
        # Get date from text
        date = self._parse_date_from_text(text)
        # Get metrics from text
        count = self._parse_metrics(text)
        record = {
            "source_url": pdf_url,
            "date": date,
            "count": count,
        }
        return record

    def _get_relevant_element(self, soup: BeautifulSoup) -> element.Tag:
        """Get the relevant element in news feed."""
        elem = soup.find(class_="file__container_item")
        if not elem:
            raise ValueError("No relevant element found, please update the script.")
        return elem

    def _parse_date_from_text(self, text: str) -> str:
        """Get date from text."""
        date = re.search(self.regex["date"], text).group(1)
        return clean_date(date, "%d/%m/%Y")

    def _get_pdf_link_from_element(self, elem: element.Tag) -> str:
        """Extract pdf link from element."""
        url = elem.find(class_="the-caption")["href"]
        return url

    def _parse_pdf_link(self, url: str) -> str:
        """Get text from the pdf link."""
        with tempfile.NamedTemporaryFile() as tmp:
            download_file_from_url(url, tmp.name)
            image = convert_from_path(tmp.name)
            text = pytesseract.image_to_string(image[0], lang="spa")
        text = re.sub(r"(\d)\,(\d)", r"\1\2", text)
        text = re.sub(r"\s+", " ", text)
        return text

    def _parse_metrics(self, text: str) -> int:
        """Get metrics from news text."""
        count = re.search(self.regex["count"], text).group(1)
        return clean_count(count)

    def export(self):
        """Export data to csv."""
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
    DominicanRepublic().export()


if __name__ == "__main__":
    main()
