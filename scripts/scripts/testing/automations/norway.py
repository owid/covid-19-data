"""Constructs daily time series of COVID-19 testing data for Norway.
Dashboard: https://www.fhi.no/en/id/infectious-diseases/coronavirus/daily-reports/daily-reports-COVID19/
"""
import re
import json
import requests
import pandas as pd

COUNTRY = 'Norway'
UNITS = 'people tested'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Norwegian Institute of Public Health'
SOURCE_URL = 'https://www.fhi.no/en/id/infectious-diseases/coronavirus/daily-reports/daily-reports-COVID19'
DATA_URL = 'https://www.fhi.no/api/chartdata/api/90789'

# sample of official values for cross-checking against the API data.
sample_official_data = [
    ("2020-09-04", {"daily_change": 12188 + 116, "source": "https://www.fhi.no/en/id/infectious-diseases/coronavirus/daily-reports/daily-reports-COVID19"}),
    ("2020-08-02", {"daily_change": 1440 + 13, "source": "https://www.fhi.no/en/id/infectious-diseases/coronavirus/daily-reports/daily-reports-COVID19"}),
    ("2020-06-26", {"daily_change": 3393 + 22, "source": "https://www.fhi.no/en/id/infectious-diseases/coronavirus/daily-reports/daily-reports-COVID19"}),
    ("2020-04-07", {"daily_change": 2592 + 151, "source": "https://www.fhi.no/en/id/infectious-diseases/coronavirus/daily-reports/daily-reports-COVID19"}),
    ("2020-03-02", {"daily_change": 383 + 8, "source": "https://www.fhi.no/en/id/infectious-diseases/coronavirus/daily-reports/daily-reports-COVID19"}),
    ("2020-02-24", {"daily_change": 12, "source": "https://www.fhi.no/en/id/infectious-diseases/coronavirus/daily-reports/daily-reports-COVID19"}),
]

def main() -> None:
    df = get_data()
    df.sort_values('Date', inplace=True)
    df['Country'] = COUNTRY
    df['Units'] = UNITS
    df['Testing type'] = TESTING_TYPE
    df['Source URL'] = SOURCE_URL
    df['Source label'] = SOURCE_LABEL
    df['Notes'] = ""
    sanity_checks(df)
    df = df[['Country', 'Units', 'Testing type', 'Date', 'Daily change in cumulative total', 'Source URL', 'Source label', 'Notes']]
    df.to_csv("automated_sheets/Norway.csv", index=False)
    return None

def get_data() -> pd.DataFrame:
    res = requests.get(DATA_URL)
    assert res.ok
    json_data = json.loads(res.text)
    df = pd.DataFrame(json_data[1:], columns=json_data[0])
    df['Negative'] = df['Negative'].astype(int)
    df['Positive'] = df['Positive'].astype(int)
    df['Daily change in cumulative total'] = df['Negative'] + df['Positive']
    return df

def sanity_checks(df: pd.DataFrame) -> None:
    """checks that there are no obvious errors in the scraped data.
    """
    # checks that the cumulative number of tests on date t is always greater than the figure for t-1:
    df_cp = df.copy()
    df_cp['Cumulative total'] = df_cp['Daily change in cumulative total'].cumsum()
    assert (df_cp['Cumulative total'].iloc[1:] >= df_cp['Cumulative total'].shift(1).iloc[1:]).all(), "On one or more dates, `Cumulative total` is greater on date t-1."
    # cross-checks a sample of scraped figures against the expected result.
    assert len(sample_official_data) > 0
    for dt, d in sample_official_data:
        val = df_cp.loc[df['Date'] == dt, 'Daily change in cumulative total'].squeeze().sum()
        assert val == d['daily_change'], f"scraped value ({val:,d}) != official value ({d['daily_change']:,d}) on {dt}"
    return None

if __name__ == '__main__':
    main()
