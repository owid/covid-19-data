"""constructs a daily time series for Finland of the daily change in COVID-19 tests.
API documentation: https://thl.fi/fi/tilastot-ja-data/aineistot-ja-palvelut/avoin-data/varmistetut-koronatapaukset-suomessa-covid-19-
"""

import json
import requests
import pandas as pd
import numpy as np

def main():
    url = "https://sampo.thl.fi/pivot/prod/fi/epirapo/covid19case/fact_epirapo_covid19case.json?row=dateweek2020010120201231-443702L&column=measure-445356"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'}

    # retrieves data
    res = requests.get(url, headers=headers)
    assert res.ok
    json_data = json.loads(res.content)

    # extracts dates
    s_dates = (
        pd.DataFrame(json_data["dataset"]["dimension"]["dateweek2020010120201231"]["category"])
        .set_index("index")
        .squeeze()
    )
    s_dates.index = s_dates.index.astype(str)
    s_dates.name = "Date"

    # extracts daily test values
    s_daily_tests = pd.Series(json_data["dataset"]["value"]).sort_index().astype(float)
    s_daily_tests.index = s_daily_tests.index.astype(str)
    s_daily_tests.name = "Daily change in cumulative total"

    df = (
        pd.merge(s_dates, s_daily_tests, left_index=True, right_index=True, how="outer")
        .sort_values("Date")
    )

    df = df[-df["Daily change in cumulative total"].isnull()]
    df.loc[:, "Country"] = "Finland"
    df.loc[:, "Units"] = "samples tested"
    df.loc[:, "Source URL"] = "https://experience.arcgis.com/experience/d40b2aaf08be4b9c8ec38de30b714f26"
    df.loc[:, "Source label"] = "Finnish Department of Health and Welfare"
    df.loc[:, "Notes"] = np.nan
    df.loc[:, "Testing type"] = "PCR only"

    df.to_csv("automated_sheets/Finland.csv", index=False)

if __name__ == '__main__':
    main()
