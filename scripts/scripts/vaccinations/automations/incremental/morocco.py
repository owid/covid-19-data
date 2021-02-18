import pandas as pd
import vaxutils
import datetime
import os
import re
import PyPDF2


def read(source: str) -> pd.Series:
    return parse_data(source)


def parse_data(source: str) -> pd.Series:
    os.system(f"curl {source} -o morocco.pdf -s")
    with open("morocco.pdf", "rb") as pdfFileObj:
        pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
        text = pdfReader.getPage(0).extractText()

    keys = ("source_url", "total_vaccinations")
    values = (source, parse_total_vaccinations(text))
    data = dict(zip(keys, values))
    return pd.Series(data=data)


def parse_total_vaccinations(text: str) -> int:
    regex = r"Bénéficiaires de la vaccination\s+Cumul global([\d\s]+)Situation épidémiologique"
    total_vaccinations = re.search(regex, text)
    total_vaccinations = vaxutils.clean_count(total_vaccinations.group(1))
    return total_vaccinations


def format_date(input: pd.Series) -> pd.Series:
    date = datetime.date.today() - datetime.timedelta(days=1)
    date = str(date)
    return vaxutils.enrich_data(input, 'date', date)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Morocco")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Oxford/AstraZeneca, Sinopharm/Beijing")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url',
                                "https://www.gov.bm/sites/default/files/COVID-19%20Vaccination%20Updates.pdf")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
    )


def main():
    dt = datetime.date.today() - datetime.timedelta(days=1)
    url_date = dt.strftime("%-d.%-m.%y")
    source = f"http://www.covidmaroc.ma/Documents/BULLETIN/{url_date}.COVID-19.pdf"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data['location'],
        total_vaccinations=data['total_vaccinations'],
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )
    os.remove("morocco.pdf")


if __name__ == "__main__":
    main()
