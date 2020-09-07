"""Constructs daily time series of COVID-19 testing data for Sri Lanka.
API endpoint: https://www.hpb.health.gov.lk/api/get-current-statistical
"""

import re
import json
import requests
import pandas as pd

COUNTRY = 'Sri Lanka'
UNITS = 'tests performed'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Sri Lanka Health Promotion Bureau'
SOURCE_URL = 'https://www.hpb.health.gov.lk'
DATA_URL = 'https://www.hpb.health.gov.lk/api/get-current-statistical'

# sample of official values for cross-checking against the API data.
official_cumulative_totals = [
    ("2020-09-03", {"cumulative_total": 235221, "source": "http://web.archive.org/web/20200904152730/https://www.hpb.health.gov.lk/en"}),
    ("2020-04-30", {"cumulative_total": 22418, "source": "http://web.archive.org/web/20200715071651/https://www.hpb.health.gov.lk/en"}),
    ("2020-04-25", {"cumulative_total": 15393, "source": "http://web.archive.org/web/20200519033637/https://www.hpb.health.gov.lk/en"}),
    ("2020-03-05", {"cumulative_total": 41, "source": "http://web.archive.org/web/20200904152730/https://www.hpb.health.gov.lk/en"}),
    ("2020-02-18", {"cumulative_total": 1, "source": "http://web.archive.org/web/20200904152730/https://www.hpb.health.gov.lk/en"}),
]

def main() -> None:
    df = get_data()
    df = df.sort_values('Date')
    df['Country'] = COUNTRY
    df['Units'] = UNITS
    df['Testing type'] = TESTING_TYPE
    df['Source URL'] = SOURCE_URL
    df['Source label'] = SOURCE_LABEL
    df['Notes'] = ""
    sanity_checks(df)
    df = df[['Country', 'Units', 'Testing type', 'Date', 'Daily change in cumulative total', 'Source URL', 'Source label', 'Notes']]
    df.to_csv("automated_sheets/Sri Lanka.csv", index=False)

def get_data() -> pd.DataFrame:
    res = requests.get(DATA_URL)
    assert res.ok
    json_data = json.loads(res.text)
    df = pd.DataFrame(json_data['data']['daily_pcr_testing_data'])
    df = df.rename(columns={'count': 'Daily change in cumulative total', 'date': 'Date'})
    df['Daily change in cumulative total'] = df['Daily change in cumulative total'].astype(int)
    df = df[df['Daily change in cumulative total'] > 0]
    assert json_data['data']['total_pcr_testing_count'] == df['Daily change in cumulative total'].sum(), 'Sum of daily changes does not equal cumulative total.'
    return df

def sanity_checks(df: pd.DataFrame) -> None:
    """checks that there are no obvious errors in the scraped data.
    """
    # checks that the cumulative number of tests on date t is always greater than the figure for t-1:
    df_cp = df.copy()
    df_cp['Cumulative total'] = df_cp['Daily change in cumulative total'].cumsum()
    assert (df_cp['Cumulative total'].iloc[1:] >= df_cp['Cumulative total'].shift(1).iloc[1:]).all(), "On one or more dates, `Cumulative total` is greater on date t-1."
    # cross-checks a sample of scraped figures against the expected result.
    assert len(official_cumulative_totals) > 0
    for dt, d in official_cumulative_totals:
        val = df_cp.loc[df['Date'] == dt, 'Cumulative total'].squeeze().sum()
        assert val == d['cumulative_total'], f"scraped value ({val:,d}) != official value ({d['cumulative_total']:,d}) on {dt}"

if __name__ == '__main__':
    main()
