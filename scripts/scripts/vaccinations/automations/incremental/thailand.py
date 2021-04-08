import re
import requests

from bs4 import BeautifulSoup
import pandas as pd
from pdfreader import SimplePDFViewer
import tempfile

import vaxutils


def read(source: str) -> pd.Series:
    yearly_report_page = BeautifulSoup(requests.get(source).content, "html.parser")
    # Get Newest Month Report Page
    monthly_report_link = yearly_report_page.find("div", class_="col-lg-12", id="content-detail").find("a")["href"]
    monthly_report_page = BeautifulSoup(requests.get(monthly_report_link).content, "html.parser")
    return parse_data(monthly_report_page)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    # Get Newest PDF Report Link
    lastest_report_link = soup.find("div", class_="col-lg-12", id="content-detail").find("a")["href"]

    tf = tempfile.NamedTemporaryFile()

    with open(tf.name, mode="wb") as f:
        f.write(requests.get(lastest_report_link).content)

    with open(tf.name, mode="rb") as f:
        viewer = SimplePDFViewer(f)
        viewer.render()
        raw_text = "".join(viewer.canvas.strings)

    special_char_replace = {
        '\uf701': u'\u0e34',
        '\uf702': u'\u0e35',
        '\uf703': u'\u0e36',
        '\uf704': u'\u0e37', 
        '\uf705': u'\u0e48',
        '\uf706': u'\u0e49',
        '\uf70a': u'\u0e48',
        '\uf70b': u'\u0e49',
        '\uf70e': u'\u0e4c',
        '\uf710': u'\u0e31',
        '\uf712': u'\u0e47',
        '\uf713': u'\u0e48',
        '\uf714': u'\u0e49'
    }

    # Correct Thai Sprcial Character Error
    special_char_replace = dict((re.escape(k), v) for k, v in special_char_replace.items()) 
    pattern = re.compile("|".join(special_char_replace.keys()))
    text = pattern.sub(lambda m: special_char_replace[re.escape(m.group(0))], raw_text)

    total_vaccinations_regex = r"ผู้ที่ได้รับวัคซีนสะสม (.{1,100}) ทั้งหมด (.{1,10}) โดส"
    total_vaccinations = re.search(total_vaccinations_regex, text).group(2)
    total_vaccinations = vaxutils.clean_count(total_vaccinations)

    people_vaccinated_regex = r"ผู้ได้รับวัคซีนเข็มที่ 1 (.{1,3})นวน (.{1,10}) ราย"
    people_vaccinated = re.search(people_vaccinated_regex, text).group(2)
    people_vaccinated = vaxutils.clean_count(people_vaccinated)

    people_fully_vaccinated_regex = r"นวนผู้ได้รับวัคซีนครบตามเกณฑ์ \(ได้รับวัคซีน 2 เข็ม\) (.{1,3})นวน (.{1,10}) ราย"
    people_fully_vaccinated = re.search(people_fully_vaccinated_regex, text).group(2)
    people_fully_vaccinated = vaxutils.clean_count(people_fully_vaccinated)

    thai_date_regex = r"\( ข้อมูล ณ วันที่ (.{1,30}) เวลา (.{1,10}) น. \)"
    thai_date = re.search(thai_date_regex, text).group(1)
    thai_date_replace = {
        "มกราคม":"January",
        "กุมภาพันธ์":"February",
        "มีนาคม":"March",
        "เมษายน":"April",
        "พฤษภาคม":"May",
        "มิถุนายน":"June",
        "กรกฎาคม":"July",
        "สิงหาคม":"August",
        "กันยายน":"September",
        "ตุลาคม":"October",
        "พฤศจิกายน":"November",
        "ธันวาคม":"December",
        "2563":"2020",
        "2564":"2021",
        "2565":"2022",
        "2566":"2023",
        "2567":"2024"
    }

    # Replace Thai Date Format with Standard Date Time Format
    thai_date_replace = dict((re.escape(k), v) for k, v in thai_date_replace.items()) 
    pattern = re.compile("|".join(thai_date_replace.keys()))
    date = pattern.sub(lambda m: thai_date_replace[re.escape(m.group(0))], thai_date)
    date = vaxutils.clean_date(date, "%d %B %Y")

    return pd.Series(data={
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated" : people_fully_vaccinated,
        "date": date,
        "source_url": lastest_report_link,
    })


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "location", "Thailand")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, "vaccine", "Oxford/AstraZeneca, Sinovac")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main():
    source = "https://ddc.moph.go.th/dcd/pagecontent.php?page=643&dept=dcd"

    # Since it a PDF Report / Text Format might be change and cause error
    try:
      data = read(source).pipe(pipeline)
      vaxutils.increment(
          location=data["location"],
          total_vaccinations=data["total_vaccinations"],
          people_vaccinated=data["people_vaccinated"],
          people_fully_vaccinated=data["people_fully_vaccinated"],
          date=data["date"],
          source_url=data["source_url"],
          vaccine=data["vaccine"]
      )
    except:
      print("An error occurred : Text format might change - need further investigation")


if __name__ == "__main__":
    main()
