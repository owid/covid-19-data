"""Constructs daily time series of COVID-19 testing data for Mauritania."""

import re
import requests
import datetime
import calendar
from typing import Tuple, List
from io import BytesIO
import pandas as pd
from bs4 import BeautifulSoup
import pdfplumber


COUNTRY = 'Mauritania'
UNITS = 'tests performed'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Ministry of Health'
URL = 'http://www.sante.gov.mr'
SERIES_TYPE = 'Daily change in cumulative total'  # one of: {'Cumulative total', 'Daily change in cumulative total'}
FILEPATH = "automated_sheets/Mauritania.csv"

MONTH_FRA2ENG = {
    'janvier': 'january',
    'février': 'february',
    'mars': 'march',
    'avril': 'april',
    'mai': 'may',
    'juin': 'june',
    'juillet': 'july',
    'août': 'august',
    'aout': 'august',
    'septembre': 'september',
    'octobre': 'october',
    'novembre': 'november',
    'décembre': 'december',
}
MONTH_NAME2INT = {v.lower(): k for k,v in enumerate(calendar.month_name)}


def merge_old_new(old, new):
    df = pd.concat([old, new]).dropna(subset=[SERIES_TYPE])
    df = df.sort_values(["Date", SERIES_TYPE]).groupby("Date").tail(1)
    df[SERIES_TYPE] = df[SERIES_TYPE].astype(int)
    return df


def main() -> None:
    old = pd.read_csv(FILEPATH)
    new = get_data()
    new['Country'] = COUNTRY
    new['Units'] = UNITS
    new['Testing type'] = TESTING_TYPE
    new['Source label'] = SOURCE_LABEL
    new['Notes'] = pd.NA
    df = merge_old_new(old, new)
    df.loc[:, "Cumulative total"] = pd.NA
    df.to_csv(FILEPATH, index=False)
    return None


def get_data() -> List[str]:
    sitreport_urls = _get_sitreport_urls()
    sitreport_urls = sitreport_urls[:3]
    data = []
    for url in sitreport_urls:
        tests_cumul = None
        tests_today = None
        tests_today_pcr = None
        date = None
        try:
            text, date = _extract_sitreport_text_and_date(url)
            tests_cumul, tests_today, tests_today_pcr = _extract_sitreport_tests_performed(text)
        except Exception as e:
            print(f'Failed to extract tests performed from situation report {url}. Error: {e}')
        row = {'Source URL': url, 'Date': date, 'cumulative_total': tests_cumul,
               'daily_change': tests_today, 'daily_change_pcr': tests_today_pcr}
        data.append(row)
        # print(row)
    df = pd.DataFrame(data)
    df.rename(columns={'daily_change_pcr': 'Daily change in cumulative total'}, inplace=True)
    # subtracts 1 day from PDFs with a creation hour before 6am. This is
    # to ensure consistency with the date shown on the situation report
    # itself.
    df['Date'] = df['Date'].apply(lambda dt: dt-datetime.timedelta(days=1) if dt.hour < 6 else dt)
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df = df[['Date', SERIES_TYPE, 'Source URL']]
    df.sort_values('Date', inplace=True)
    df.dropna(subset=['Date', SERIES_TYPE], how='any', inplace=True)
    df[SERIES_TYPE] = df[SERIES_TYPE].astype(int)
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None, 'display.width', None):
    #     print(df)
    return df
            

def _get_sitreport_urls() -> List[str]:
    """retrieves a list of URLs for the daily situation reports."""
    params = {'cat': 4, 'paged': 1}
    # retrieves the U
    res = requests.get(URL, params=params)
    soup = BeautifulSoup(res.text, 'html.parser')
    n_pages = 1
    pdf_urls = []
    checked_urls = set({})
    for i in range(n_pages):
        params['paged'] = 1+i
        res = requests.get(URL, params=params)
        soup = BeautifulSoup(res.text, 'html.parser')
        page_urls = [a.get('href') for a in soup.find('div', {'class': 'post-grid'}).find_all('a') if re.search(r'covid', a.get('title'), re.IGNORECASE)]
        for url in page_urls:
            try:
                if url not in checked_urls:
                    res = requests.get(url)
                    soup = BeautifulSoup(res.text, 'html.parser')
                    # date = _extract_sitreport_date(soup)
                    pdf_url = _extract_sitreport_url(soup)
                    if pdf_url:
                        pdf_urls.append(pdf_url)
            except Exception as e:
                print(f'Failed to retrieve PDF url from page {url}. Error: {e}')
            checked_urls.add(url)
    return pdf_urls


def _extract_sitreport_url(soup: BeautifulSoup) -> str:
    """given html soup of a press release, returns the url of the
    corresponding PDF situation report."""
    pdf_url = None
    regex_res = re.search(r'covid', soup.find('div', {'id': 'post-print-area'}).text, re.IGNORECASE)
    if regex_res:
        a = soup.find('div', {'class': 'post-content'}).find('a')
        if a:
            pdf_url = a.get('href')
            if not pdf_url.endswith('.pdf'):
                pdf_url = None
    return pdf_url


def _extract_sitreport_text_and_date(url: str) -> Tuple[str, datetime.datetime]:
    """returns the text and date from a PDF situation report."""
    text = ''
    date = None
    res = requests.get(url)
    with pdfplumber.open(BytesIO(res.content)) as pdf:
        # extracts text from the PDF.
        for page in pdf.pages:
            page_text = page.extract_text()
            text += '\n' + page_text
        # extracts date from the pdf.
        date = datetime.datetime.strptime(pdf.metadata['CreationDate'][2:-7], '%Y%m%d%H%M%S')
    return text, date


def _extract_sitreport_tests_performed(text: str) -> Tuple[int, int, int]:
    """extracts the number of tests performed from a single situation
    report.

    Returns:

        Tuple[int, int, int]: 
            tests_cumul: cumulative number of tests performed to date.
            tests_today: number of tests performed in past 24 hours.
            tests_today_pcr: number of "diagnostic" tests performed in the past
                24 hours (tests_today_pcr is strictly less than or equal to
                tests_today).
    """
    tests_cumul = None
    tests_today = None
    tests_today_pcr = None
    tests_cumul_regex_res = re.search(r'(\d+,?\d*) tests ont été effectués', text, re.IGNORECASE)
    if tests_cumul_regex_res:
        tests_cumul = tests_cumul_regex_res.groups()[0]
    tests_today_regex_res = re.search(r'(\d+,?\d*)\D{1,50}aujourd’hui', text, re.IGNORECASE)
    if tests_today_regex_res:
        tests_today = tests_today_regex_res.groups()[0]
    tests_today_pcr_regex_res = re.search(r'aujourd’hui\D{1,20}(\d+,?\d*)\D{1,20}diagnostic', text, re.IGNORECASE)
    if tests_today_pcr_regex_res:
        tests_today_pcr = tests_today_pcr_regex_res.groups()[0]
    if tests_today and tests_today_pcr:
        assert int(tests_today_pcr) <= int(tests_today), f'`tests_today_pcr` ({tests_today_pcr}) should be less than or equal to `tests_today` ({tests_today})'
    return tests_cumul, tests_today, tests_today_pcr


if __name__ == '__main__':
    main()
