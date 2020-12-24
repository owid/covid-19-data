import re
import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup

def main():

    data = pd.read_csv("Jamaica.csv")

    # get and parse daily updates page  
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
    general_url = "https://www.moh.gov.jm/updates/coronavirus/covid-19-clinical-management-summary/"

    req = requests.get(general_url, headers=headers)
    soup = BeautifulSoup(req.content, 'html.parser')

    # find and assign url
    source_url = soup.find('div', class_='block-content').find('h2').find('a').attrs['href']

    # find and assign date
    date = soup.find('div', class_='block-content').find('h2').text
    date = re.search('(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(\d{1,2})\,\s+(\d{4})', date).group(0)
    date = pd.Series(pd.to_datetime(date)).dt.date.astype(str)

    # get and parse table; find and assign values
    quests = requests.get(source_url, headers=headers)
    table = pd.read_html(quests.text, index_col=0)[0]

    total = int(table.loc['Samples Tested'][2])
    pending = int(table.loc['Results Pending'][2])
    cumulative_total = total - pending

    # create and append new row
    new = pd.DataFrame({
            "Cumulative total": cumulative_total,
            "Date": date,
            "Country": "Jamaica",
            "Units": "samples tested",
            "Testing type": "PCR only",
            "Source URL": source_url,
            "Source label": "Jamaica Ministry of Health and Wellness"
        }, index=[0])

    df = pd.concat([new, data], sort=False)
    df.to_csv("Jamaica.csv", index=False)

if __name__ == '__main__':
    main()
