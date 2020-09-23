import re
import requests
import datetime
from bs4 import BeautifulSoup
import pandas as pd

def main():
    data = pd.read_csv("automated_sheets/Indonesia.csv")

    url = "https://covid19.disiplin.id/"
    req = requests.get(url)
    soup = BeautifulSoup(req.content, "html.parser")

    date = soup.find("header", class_="border-title").find("span").text
    date = re.search(r"Update hingga (\d+[^\d]+202\d)", date).group(1)
    date = pd.Series(pd.to_datetime(date)).dt.date.astype(str)
    
    if data.Date.max() < date[0]:

        count = soup.find("div", class_="box-right").find("div", class_="global-area").find("h4").text
        count = int(count.replace(".", ""))

        if count > data["Cumulative total"].max():

            new = pd.DataFrame({
                "Cumulative total": count,
                "Date": date,
                "Country": "Indonesia",
                "Units": "people tested",
                "Testing type": "unclear",
                "Source URL": url,
                "Source label": "Emerging infections, Indonesian Ministry of Health"
            })

            df = pd.concat([new, data], sort=False)
            df.to_csv("automated_sheets/Indonesia.csv", index=False)

if __name__ == '__main__':
    main()
