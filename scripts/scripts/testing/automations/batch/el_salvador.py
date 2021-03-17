import re
import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

data = pd.read_csv("automated_sheets/El Salvador.csv")

date = datetime(2020, 3, 16)
enddate = datetime.today()

while date < enddate:

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
    source_url = "https://diario.innovacion.gob.sv/?_token=PuE3sMLbt2Yu5VnljNa3scEuFYew5wp4VEBHcww2&fechaMostrar="+date.strftime('%d-%m-%Y')

    req = requests.get(source_url, headers=headers)
    soup = BeautifulSoup(req.content, 'html.parser')

    daily = soup.find('div', class_='col-4 col-sm-2 col-lg-2 align-self-center offset-lg-0').find('label').text.strip()

    new = pd.DataFrame({
        "Date": [date],
        "Daily change in cumulative total": daily,
        "Country": 'El Salvador',
        "Units": 'tests performed',
        "Source_label": 'Government of El Salvador',
        "Source_URL": source_url
        })

    data = pd.concat([new, data], sort=False)
    
    date+=timedelta(days=1)

data['Daily change in cumulative total'].replace(['0'],[''], inplace=True)
data['Daily change in cumulative total'].replace('', np.nan, inplace=True)
data.dropna(subset = ["Daily change in cumulative total"], inplace=True)
data.to_csv("automated_sheets/El Salvador.csv", index = False)
