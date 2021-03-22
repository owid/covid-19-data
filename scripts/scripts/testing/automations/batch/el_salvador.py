import datetime
import requests

from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

data = pd.read_csv("automated_sheets/El Salvador.csv")

date = pd.to_datetime(data.Date.max()).date()
enddate = datetime.date.today()

while date < enddate:

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
    }
    source_url = f"https://diario.innovacion.gob.sv/?fechaMostrar={date.strftime('%d-%m-%Y')}"

    try:
        req = requests.get(source_url, headers=headers, timeout=10)
    except Exception as e:
        req = requests.get(source_url, headers=headers, timeout=10)

    soup = BeautifulSoup(req.content, "html.parser")

    daily = pd.to_numeric(
        soup
        .find("div", class_="col-4 col-sm-2 col-lg-2 align-self-center offset-lg-0")
        .find("label")
        .text.strip()
    )

    new = pd.DataFrame({
        "Date": [str(date)],
        "Daily change in cumulative total": daily,
        "Country": "El Salvador",
        "Units": "tests performed",
        "Source label": "Government of El Salvador",
        "Source URL": source_url
    })

    # print(f"{date} - {daily}")

    data = pd.concat([data, new])
    data = data[data["Daily change in cumulative total"].fillna(0) > 0]
    data.drop_duplicates().to_csv("automated_sheets/El Salvador.csv", index=False)

    date += datetime.timedelta(days=1)
