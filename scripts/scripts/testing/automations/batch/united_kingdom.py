from uk_covid19 import Cov19API
from typing import Union
import pandas as pd

# MAX_DATE_FILL_PILLAR_ONE_TWO: the max date where it is ok to fill NA
#   values of cumVirusTests/newVirusTests with
#   cumPillarOneTwoTestsByPublishDate/newPillarOneTwoTestsByPublishDate.
#   Prior to (and including this date),
#   cumPillarOneTwoTestsByPublishDate/newPillarOneTwoTestsByPublishDate
#   was equal to cumVirusTests/newVirusTests every day, so we presume it
#   is safe to fill in NA values of cumVirusTests/newVirusTests (which
#   allows the time series to extend a few weeks further back in time).
MAX_DATE_FILL_PILLAR_ONE_TWO = '2020-05-03' 


def main():
    # retrieves daily time series data from the API.
    pcr_testing = {
        "Date": "date",
        "cumPillarOneTwoTestsByPublishDate": "cumPillarOneTwoTestsByPublishDate",
        "newPillarOneTwoTestsByPublishDate": "newPillarOneTwoTestsByPublishDate",
        "cumVirusTests": "cumVirusTests",
        "newVirusTests": "newVirusTests",
    }
    api = Cov19API(filters=["areaType=overview"], structure=pcr_testing)
    df = api.get_dataframe()
    
    # fills NA values of `cumVirusTests` and `newVirusTests` prior to
    # MAX_DATE_FILL_PILLAR_ONE_TWO using
    # `cumPillarOneTwoTestsByPublishDate` and
    # `newPillarOneTwoTestsByPublishDate`.
    df['cumVirusTests2'] = df.apply(lambda row: _fill_virus_tests(row, 'cum', MAX_DATE_FILL_PILLAR_ONE_TWO), axis=1)
    df['newVirusTests2'] = df.apply(lambda row: _fill_virus_tests(row, 'new', MAX_DATE_FILL_PILLAR_ONE_TWO), axis=1)
    
    # assigns `Notes` column
    note1 = ('Constructed from the newVirusTests/cumVirusTests variables')
    note2 = ('Sum of tests processed for pillars 1 and 2 '
             '(cumPillarOneTwoTestsByPublishDate/newPillarOneTwoTestsByPublishDate')
    notes = []
    for _, row in df.iterrows():
        note = None
        if pd.notnull(row['cumVirusTests2']):
            if pd.notnull(row['cumVirusTests']):
                note = note1
            elif pd.notnull(row['cumPillarOneTwoTestsByPublishDate']):
                note = note2
        notes.append(note)
    
    assert len(notes) == df.shape[0]
    df['Notes'] = notes
    
    # renames and subsets to final columns
    df.rename(columns={
        "cumVirusTests2": "Cumulative total",
        "newVirusTests2": "Daily change in cumulative total"
    }, inplace=True)
    df = df[["Date", "Cumulative total", "Daily change in cumulative total", "Notes"]]
    
    # assigns constants
    df.loc[:, "Country"] = "United Kingdom"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Source label"] = "Department of Health and Social Care and Public Health England"
    df.loc[:, "Source URL"] = "https://coronavirus.data.gov.uk/developers-guide"
    
    df.to_csv("automated_sheets/United Kingdom - tests.csv", index=False)


def _fill_virus_tests(row: Union[pd.Series, dict], new_or_cum: str, max_date: str) -> Union[int, float]:
    """fills NA values of newVirusTests prior to `max_date` using
    newPillarOneTwoTestsByPublishDate OR fills umVirusTests prior to
    `max_date` using cumPillarOneTwoTestsByPublishDate.

    Arguments:

        row: Union[pd.Series, dict]. Pandas series or dict representing a 
            single day of data. Example:

            {
                'Date': '2020-12-17', 
                'cumPillarOneTwoTestsByPublishDate': 43485398, 
                'cumVirusTests': 4677136
                }

        new_or_cum: str. If "new", fills NA values of newVirusTests prior to 
            `max_date` using newPillarOneTwoTestsByPublishDate. If "cum", 
            fills NA values of cumVirusTests prior to `max_date` using 
            cumPillarOneTwoTestsByPublishDate.

        max_date: str. Date in YYYY-MM-DD format. Only fills 
            newVirusTests/cumVirusTests for days on or prior to this date.

    Returns:

        val: Union[int, float]. The filled value of 
            newVirusTests/cumVirusTests.
    """
    assert new_or_cum in ['new', 'cum'], "`new_or_cum` must be one of 'new' or 'cum'"
    val = row[f'{new_or_cum}VirusTests']
    condition = row['Date'] <= max_date and \
                pd.isnull(row[f'{new_or_cum}VirusTests']) and \
                pd.notnull(row[f'{new_or_cum}PillarOneTwoTestsByPublishDate'])
    if condition:
        val = row[f'{new_or_cum}PillarOneTwoTestsByPublishDate']
    return val


if __name__ == '__main__':
    main()

