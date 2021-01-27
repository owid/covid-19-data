import re
import requests
import datetime
from bs4 import BeautifulSoup
import pandas as pd
import dateparser


SOURCE_URL = "https://ncdc.org.ly/Ar"


def libya_get_tests_snapshot():
    url = SOURCE_URL
    headers = {
        'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.80 Safari/537.36'
    }
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.content, 'html.parser')
    # retrieves the daily testing figure.
    s = 'عدد العينات'
    tests_today_str = None
    td_to_search = soup.find_all('td')
    while tests_today_str is None and len(td_to_search):
        td = td_to_search.pop(0)
        if td and s in td.text:
            regex_res = re.search(r'([\d,]+)', td.text)
            if regex_res:
                tests_today_str = regex_res.groups()[0]
    assert tests_today_str is not None, 'Failed to find daily change testing figure.'
    tests_today = int(re.sub(r'[\s,]*', '', tests_today_str))
    # retrieves the date that the data was updated.
    span = soup.find('div', {'class': 'wptb-table-container-matrix'}).find('td').find('strong')
    assert span is not None, "Failed to find span containing the date."
    regex_res = re.search(r'\d.*2021', span.text)[0]
    date = str(dateparser.parse(regex_res, languages=["ar"]).date())
    return date, tests_today


def main():
    date, tests_today = libya_get_tests_snapshot()

    existing = pd.read_csv("automated_sheets/Libya.csv")

    if date > existing["Date"].max():

        new = pd.DataFrame({
            "Country": "Libya",
            "Date": [date],
            "Daily change in cumulative total": tests_today,
            "Source URL": SOURCE_URL,
            "Source label": "Libya National Centre for Disease Control",
            "Testing type": "PCR only",
            "Units": "samples tested",
            "Notes": pd.NA
        })

        df = pd.concat([new, existing]).sort_values("Date", ascending=False)
        df.loc[:, "Cumulative total"] = pd.NA
        df.to_csv("automated_sheets/Libya.csv", index=False)


if __name__ == '__main__':
    main()
