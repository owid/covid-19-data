"""Constructs daily time series of COVID-19 testing data for El Salvador.

Dashboard: https://covid-19-gis-hub-el-salvador-esri-sv.hub.arcgis.com/

Sources of official data: https://www.facebook.com/nayibbukele ;
https://covid19.gob.sv/

Notes:

    * There is some data in the ArcGIS time series that is available prior to 
        13 April 2020, but it is gibberish/junk, so it should be disregarded.
"""

import json
import requests
import datetime
import pandas as pd

COUNTRY = 'El Salvador'
UNITS = 'tests performed'
TESTING_TYPE = 'PCR only'
SOURCE_LABEL = 'Government of El Salvador'
SOURCE_URL = 'https://covid-19-gis-hub-el-salvador-esri-sv.hub.arcgis.com/'

DATA_URL = 'https://services.arcgis.com/8TF1bhVJYrFsuHwX/arcgis/rest/services/CASOS_TOTAL_ACUMULADO/FeatureServer/0/query'
PARAMS = {
    'f': 'json',
    'where': "FECHA>timestamp '2020-04-13 00:00:00'",
    'returnGeometry': False,
    'spatialRel': 'esriSpatialRelIntersects',
    'outFields': 'OBJECTID,PRUEBAS,FECHA',
    'orderByFields': 'FECHA asc',
    'resultOffset': 0,
    'resultRecordCount': 32000,
    'resultType': 'standard',
}

# hardcoded values
hardcoded_data = [
    {'Date': "2020-04-13", "Cumulative total": 7230, "Source URL": "https://www.facebook.com/nayibbukele/posts/2818681048218068"},  # b/c data entry error
    {'Date': "2020-04-12", "Cumulative total": 6729, "Source URL": "https://www.facebook.com/nayibbukele/posts/2816238208462352"},  # b/c missing data
    {'Date': "2020-04-11", "Cumulative total": 6272, "Source URL": "https://www.facebook.com/nayibbukele/posts/2813793385373501"},  # b/c missing data
    {'Date': "2020-04-10", "Cumulative total": 5760, "Source URL": "https://www.facebook.com/nayibbukele/posts/2811309788955194"},  # b/c missing data
]


# sample of official values for cross-checking against the unofficial data.
sample_official_data = [
    ("2020-09-07", {"cumulative_total": 335099, "source": "https://www.facebook.com/nayibbukele/posts/3221781691241333"}),
    ("2020-08-28", {"cumulative_total": 310444, "source": "https://www.facebook.com/nayibbukele/posts/3191496677603168"}),
    ("2020-08-20", {"cumulative_total": 290741, "source": "https://www.facebook.com/nayibbukele/posts/3167039666715536"}),
    ("2020-07-31", {"cumulative_total": 241359, "source": "https://www.facebook.com/nayibbukele/posts/3109057972513706"}),
    ("2020-07-11", {"cumulative_total": 192206, "source": "https://www.facebook.com/nayibbukele/posts/3050428308376673"}),
    ("2020-06-25", {"cumulative_total": 152729, "source": "https://www.facebook.com/nayibbukele/posts/3007490276003810"}),
    ("2020-06-01", {"cumulative_total": 94272, "source": "https://www.facebook.com/nayibbukele/posts/2942358345850337"}),
    ("2020-05-07", {"cumulative_total": 37306, "source": "https://www.facebook.com/nayibbukele/posts/2876410262445146"}),
    ("2020-05-01", {"cumulative_total": 26961, "source": "https://www.facebook.com/nayibbukele/posts/2860998700652969"}),
    ("2020-04-23", {"cumulative_total": 16418, "source": "https://www.facebook.com/nayibbukele/posts/2842358415850331"}),
    ("2020-04-22", {"cumulative_total": 15385, "source": "https://www.facebook.com/nayibbukele/posts/2839869609432545"}),
    ("2020-04-13", {"cumulative_total": 7230, "source": "https://www.facebook.com/nayibbukele/posts/2818681048218068"}),
    ("2020-04-10", {"cumulative_total": 5760, "source": "https://www.facebook.com/nayibbukele/posts/2811309788955194"}),
]


def main() -> None:
    df = get_data()
    df['Source URL'] = df['Source URL'].apply(lambda x: SOURCE_URL if pd.isnull(x) else x)
    df['Country'] = COUNTRY
    df['Units'] = UNITS
    df['Testing type'] = TESTING_TYPE
    df['Source label'] = SOURCE_LABEL
    df['Notes'] = ""
    sanity_checks(df)
    df = df[['Country', 'Units', 'Testing type', 'Date', 'Cumulative total', 'Source URL', 'Source label', 'Notes']]
    df.to_csv("automated_sheets/El Salvador.csv", index=False)
    return None


def get_data() -> pd.DataFrame:
    res = requests.get(DATA_URL, params=PARAMS)
    json_data = json.loads(res.text)
    df = pd.DataFrame([d['attributes'] for d in json_data['features']])
    df['FECHA'] = df['FECHA'].astype(int).apply(lambda dt: datetime.datetime.utcfromtimestamp(dt/1000))
    # substracts one day from dates where data was scraped early in the morning,
    # and then removes HH:MM:SS datetime information.
    df['Date'] = df['FECHA'].apply(lambda dt: dt - datetime.timedelta(days=1) if dt.hour < 6 else dt).dt.strftime('%Y-%m-%d')
    df.sort_values('FECHA', inplace=True)
    df.rename(columns={'PRUEBAS': 'Cumulative total'}, inplace=True)
    df['Source URL'] = None
    # removes rows that are hardcoded
    hardcoded_dates = [d['Date'] for d in hardcoded_data]
    df = df[~df['Date'].isin(hardcoded_dates)]
    df = df[['Date', 'Cumulative total', 'Source URL']]
    df_hardcoded = pd.DataFrame(hardcoded_data)
    df = pd.concat([df, df_hardcoded], axis=0, sort=False).reset_index(drop=True)
    df.sort_values('Date', inplace=True)
    df = df.dropna(subset=["Cumulative total"])
    return df


def sanity_checks(df: pd.DataFrame) -> None:
    """checks that there are no obvious errors in the scraped data.
    """
    # checks that the max date is less than or equal to tomorrow's date.
    assert datetime.datetime.strptime(df['Date'].max(), '%Y-%m-%d') < (datetime.datetime.utcnow() + datetime.timedelta(days=1))
    # checks that there are no duplicate dates
    assert df['Date'].duplicated().sum() == 0, 'One or more rows share the same date.'
    # checks that the cumulative number of tests on date t is always greater than the figure for t-1:
    assert (df['Cumulative total'].iloc[1:] >= df['Cumulative total'].shift(1).iloc[1:]).all(), "On one or more dates, `Cumulative total` is greater on date t-1."
    # cross-checks a sample of scraped figures against the expected result.
    assert len(sample_official_data) > 0
    for dt, d in sample_official_data:
        val = df.loc[df['Date'] == dt, 'Cumulative total'].squeeze().sum()
        assert val == d['cumulative_total'], f"scraped value ({val:,d}) != official value ({d['cumulative_total']:,d}) on {dt}"
    return None


if __name__ == '__main__':
    main()
