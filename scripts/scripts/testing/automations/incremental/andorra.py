import re
import requests
import datetime
from bs4 import BeautifulSoup
import pandas as pd

COUNTRY = "Andorra"
PATH = f"automated_sheets/{COUNTRY}.csv"
URL = "https://www.govern.ad/covid19/"
SOURCE_LABEL = "Tauler COVID-19, Govern d'Andorra"


def get_date(soup):
    """Get date from soup.

    Args:
        soup (bs4.BeautifulSoup): Page HTML.

    Returns:
        str: date
    """
    # Month catalan name to index
    month_map = {
        "gener": 1,
        "febrer": 2,
        "març": 3,
        "mar": 3,
        "abril": 4,
        "maig": 5,
        "juny": 6,
        "juliol": 7,
        "agost": 8,
        "setembre": 9,
        "octubre": 10,
        "novembre": 11,
        "desembre": 12
    }
    date_str = soup.find(class_="text-primary tracking-normal text-lg font-bold mb-0").text.lower()
    match = re.search(r"actualització (\d+) d(e |')([a-z]+)", date_str)
    # Get day and month from website
    day = int(match.group(1))
    month = int(month_map[match.group(3)])
    # Estimate year and build date
    year = datetime.datetime.now().year
    date = datetime.date(year, month, day)
    if date > datetime.datetime.now().date():
        date = datetime.date(year-1, month, day)
    date = date.strftime("%Y-%m-%d")
    return date


def get_count(soup):
    """Get number of tests from soup.

    Args:
        soup (bs4.BeautifulSoup): Page HTML.

    Returns:
        int: Count of tests (PCR + TMA)
    """
    tag_id = "capacidtat"  # CHECK ON THIS! It is a typo on their side, correct spelling should be "capacitat"
    values = [elem.find("span") for elem in soup.find(id=tag_id).find_all("div", class_="text-primary")]
    values = [int(x.text.replace(".", "")) for x in values]
    titles = [x.text.strip() for x in soup.find(id=tag_id).findAll("h3")]
    
    count = 0
    for value, title in zip(values, titles):
        if "PCR" in title:
            count_pcr = value
        elif "TMA" in title:
            count_tma = value
    count = count_pcr + count_tma
    
    return count


def is_404(soup):
    return "404" in soup.find("title").text


def main():
    """Main function.
    
    Update file in `PATH`.
    """
    data = pd.read_csv(PATH)

    # Retrieve HTML page (using fake header, otherwise 404 error)
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
    req = requests.get(URL, headers=headers)
    soup = BeautifulSoup(req.content, "html.parser")

    if not is_404(soup):
        date = get_date(soup)

        if data.Date.max() < date:
            count = get_count(soup)
            if count > data["Cumulative total"].max():
                new_row = {
                    "Cumulative total": count,
                    "Date": date,
                    "Country": COUNTRY,
                    "Units": "people tested",
                    "Testing type": "PCR, TMA",
                    "Source URL": URL,
                    "Source label": SOURCE_LABEL
                }
                data = data.append(new_row, ignore_index=True)
                data.to_csv(PATH, index=False)


if __name__ == '__main__':
    main()
