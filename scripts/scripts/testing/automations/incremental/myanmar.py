"""Constructs daily time series of COVID-19 testing data for Myanmar.
This module has been tested on tesseract v4.1.1.
"""

import os
import re
import requests
import datetime
import calendar
from typing import List, Tuple
from io import BytesIO
from bs4 import BeautifulSoup
import pandas as pd
import pdfplumber
import pdf2image
from PIL import Image, ImageOps
import pytesseract

# NOTE: I disable ssl warnings, which otherwise appear for each request.get()
# request to https://mohs.gov.mm/page/9575.
import urllib3
urllib3.disable_warnings()

COUNTRY = 'Myanmar'
UNITS = 'samples tested'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Myanmar Ministry of Health and Sports'
FILEPATH = 'automated_sheets/Myanmar.csv'


def main() -> None:
    old = pd.read_csv(FILEPATH)
    new = get_data()
    new['Country'] = COUNTRY
    new['Units'] = UNITS
    new['Testing type'] = TESTING_TYPE
    new['Source label'] = SOURCE_LABEL
    new['Notes'] = pd.NA
    df = merge_old_new(old, new)
    df.to_csv(FILEPATH, index=False)
    return None


def merge_old_new(old, new):
    df = pd.concat([old, new]).dropna(subset=["Cumulative total"])
    df = df.sort_values("Date").groupby("Date").head(1)
    df = df.sort_values("Cumulative total").groupby("Cumulative total").head(1)
    df["Cumulative total"] = df["Cumulative total"].astype(int)
    return df


def get_data() -> pd.DataFrame:
    url = 'https://mohs.gov.mm/page/9575'
    res = requests.get(url, verify=False)
    soup = BeautifulSoup(res.content, 'html.parser')
    link = soup.find('div', {'class': 'single-blog-text-area'}).find('table').find('a')
    date = None
    samples_cumul = None
    sitreport_url = None
    try:
        regex_res = re.search(r'covid-19:? situation report\s+\d+\s+\((\d{1,2})-(\d{1,2})-(\d{4})', link.text, re.IGNORECASE)
        if regex_res:
            sitreport_url = link.get('href')
            # NOTE: the situation report date is always one day ahead of the
            # date of the reported testing figures, so I subtract one day
            # from the situation report date.
            dd, mm, yyyy = regex_res.groups()
            date = datetime.datetime(int(yyyy), int(mm), int(dd)) - datetime.timedelta(days=1)
            samples_cumul = _extract_samples_tested(sitreport_url, date)
    except Exception as e:
        print(f'Error extracting samples tested from {(date).strftime("%Y-%m-%d")} situation report ({url}): {e}')
    data = [{'Source URL': sitreport_url, 'Date': date, 'Cumulative total': samples_cumul}]
    df = pd.DataFrame(data)
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df.dropna(how='any', inplace=True)
    df['Cumulative total'] = df['Cumulative total'].astype(int)
    df.sort_values('Date', inplace=True)
    return df


def _extract_samples_tested(url: str, date: datetime.date) -> int:
    """extracts the cumulative number of samples tested from a single
    situation report.
    """
    samples_cumul = None
    res = requests.get(url, verify=False)
    bounding_box, regex = _get_bounding_box_and_regex(date)
    with pdfplumber.open(BytesIO(res.content), pages=[1]) as pdf:
        # extracts text from the PDF.
        if bounding_box:
            text = pdf.pages[0].crop(bounding_box).extract_text()
        else:
            text = pdf.pages[0].extract_text()
        # if the text is None, then OCR the cropped PDF.
        if text is None or text == '':
            image = pdf.pages[0].crop(bounding_box).to_image(resolution=300).annotated
            text = pytesseract.image_to_string(image, lang='eng', config='--psm 6')
            # image.show()
    regex_res = regex.search(text)
    if regex_res:
        samples_cumul = int(re.sub(r'[\s,]+', '', regex_res.groups()[0]))
    return samples_cumul


def _get_bounding_box_and_regex(date):
    """retrieves the bounding box that contains the samples tested figure
    in a situation report, along with the appropriate regular expression
    for extracting the samples tested figure from the text.

    bounding box format (units=pt): (x0, top, x1, bottom)
    """
    assert isinstance(date, datetime.date), 'date must be a datetime object.'
    bounding_boxes_data = [
        {'min_date': datetime.datetime(2020, 4, 3),
         'max_date': datetime.datetime(2020, 5, 17),
         'bounding_box': (285, 230, 285+270, 230+45),
         'regex': re.compile(r'total tested specimen for covid-19\s*\(as of .{1,20}\)\s*([\d,\s]{3,})', re.IGNORECASE)},
        {'min_date': datetime.datetime(2020, 5, 18),
         'max_date': datetime.datetime(2020, 5, 20),
         'bounding_box': (285, 230, 285+270, 230+30),
         'regex': re.compile(r'total\s*t\w{4}d[^\d]{,5}([\d,\s]{3,})', re.IGNORECASE)},
        {'min_date': datetime.datetime(2020, 5, 21),
         'max_date': datetime.datetime(2020, 7, 6),
         'bounding_box': (48, 235, 48+100, 235+50),
         'regex': re.compile(r'total\s*t\w{4}d[^\d]{,5}([\d,\s]{3,})', re.IGNORECASE)},
        {'min_date': datetime.datetime(2020, 7, 7),
         'max_date': datetime.datetime.today(),
         'bounding_box': (34, 307, 34+100, 307+74),
         'regex': re.compile(r'total\s*t\w{4}d[^\d]{,5}([\d,\s]{3,})', re.IGNORECASE)},
    ]
    bounding_box = None
    regex = None
    while bounding_box is None and regex is None and len(bounding_boxes_data):
        bbox_data = bounding_boxes_data.pop(0)
        if date >= bbox_data['min_date'] and date <= bbox_data['max_date']:
            bounding_box = bbox_data['bounding_box']
            regex = bbox_data['regex']
    return bounding_box, regex


if __name__ == '__main__':
    main()
