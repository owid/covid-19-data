import re
import requests

from bs4 import BeautifulSoup
import pandas as pd
from pdfreader import SimplePDFViewer
import tempfile

from vax.utils.incremental import enrich_data, increment, clean_date, clean_count


def read(source: str) -> pd.Series:
    yearly_report_page = BeautifulSoup(requests.get(source).content, "html.parser")
    # Get Newest Month Report Page
    monthly_report_link = yearly_report_page.find("div", class_="col-lg-12", id="content-detail").find("a")["href"]
    monthly_report_page = BeautifulSoup(requests.get(monthly_report_link).content, "html.parser")
    return parse_data(monthly_report_page)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    # Get Newest PDF Report Link
    latest_report_link = soup.find("div", class_="col-lg-12", id="content-detail").find("a")["href"]

    tf = tempfile.NamedTemporaryFile()

    with open(tf.name, mode="wb") as f:
        f.write(requests.get(latest_report_link).content)

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

    total_vaccinations_regex = r"ผู้ที่ได้รับวัคซีนสะสม .{1,100} ทั้งหมด[^\d]+([\d,]+) โดส"
    total_vaccinations = re.search(total_vaccinations_regex, text).group(1)
    total_vaccinations = clean_count(total_vaccinations)

    people_vaccinated_regex = r"ผู้ได้รับวัคซีนเข็มที่ 1 .{1,3}นวน[^\d]+([\d,]+) ร.{1,3}ย"
    people_vaccinated = re.search(people_vaccinated_regex, text).group(1)
    people_vaccinated = clean_count(people_vaccinated)

    people_fully_vaccinated_regex = (
        r"นวนผู้ได้รับวัคซีนครบต.{1,2}มเกณฑ์ \(ได้รับวัคซีน 2 เข็ม\) .{1,3}นวน[^\d]+([\d,]+)"
    )
    people_fully_vaccinated = re.search(people_fully_vaccinated_regex, text).group(1)
    people_fully_vaccinated = clean_count(people_fully_vaccinated)

    thai_date_regex = r"\( ข้อมูล ณ วันที่ (.{1,30}) เวล(.{1,3}) (.{1,10}) น. \)"
    thai_date = re.search(thai_date_regex, text).group(1).replace("ำ", "า")
    thai_date_replace = {
        "มกราคม": "January",
        "กุมภาพันธ์": "February",
        "มีนาคม": "March",
        "เมษายน": "April",
        "พฤษภาคม": "May",
        "มิถุนายน": "June",
        "กรกฎาคม": "July",
        "สิงหาคม": "August",
        "กันยายน": "September",
        "ตุลาคม": "October",
        "พฤศจิกายน": "November",
        "ธันวาคม": "December",
        "2563": "2020",
        "2564": "2021",
        "2565": "2022",
        "2566": "2023",
        "2567": "2024"
    }

    # Replace Thai Date Format with Standard Date Time Format
    thai_date_replace = dict((re.escape(k), v) for k, v in thai_date_replace.items())
    pattern = re.compile("|".join(thai_date_replace.keys()))
    date = pattern.sub(lambda m: thai_date_replace[re.escape(m.group(0))], thai_date)
    date = clean_date(date, "%d %B %Y")

    return pd.Series(data={
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": date,
        "source_url": latest_report_link,
    })


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Thailand")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Johnson&Johnson, Oxford/AstraZeneca, Sinovac")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main():
    source = "https://ddc.moph.go.th/dcd/pagecontent.php?page=643&dept=dcd"

    # Since it a PDF Report / Text Format might be change and cause error
    data = read(source).pipe(pipeline)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"].replace(" ", "%20"),
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
