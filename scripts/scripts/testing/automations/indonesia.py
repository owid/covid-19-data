import requests
import datetime
from bs4 import BeautifulSoup
import pandas as pd

def main():
    data = pd.read_csv("automated_sheets/Indonesia.csv")

    today_str = str(datetime.date.today())

    if data.Date.max() < today_str:

        url = "https://covid19.disiplin.id/"

        req = requests.get(url)
        soup = BeautifulSoup(req.content, "html.parser")

        count = soup.find("div", class_="box-right").find("div", class_="global-area").find("h4").text
        count = int(count.replace(".", ""))

        if count > data["Cumulative total"].max():

            new = pd.DataFrame({
                "Cumulative total": count,
                "Date": [today_str],
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
