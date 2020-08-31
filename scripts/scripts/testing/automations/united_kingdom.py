from uk_covid19 import Cov19API

def main():
    pcr_testing = {
        "Date": "date",
        "newPillarOneTestsByPublishDate": "newPillarOneTestsByPublishDate",
        "cumPillarOneTestsByPublishDate": "cumPillarOneTestsByPublishDate",
        "newPillarTwoTestsByPublishDate": "newPillarTwoTestsByPublishDate",
        "cumPillarTwoTestsByPublishDate": "cumPillarTwoTestsByPublishDate"
    }

    api = Cov19API(filters=["areaType=overview"], structure=pcr_testing)
    df = api.get_dataframe()

    df.loc[:, "Cumulative total"] = (
        df.cumPillarOneTestsByPublishDate
        .add(df.cumPillarTwoTestsByPublishDate)
    )

    df.loc[:, "Daily change in cumulative total"] = (
        df.newPillarOneTestsByPublishDate
        .add(df.newPillarTwoTestsByPublishDate)
    )

    df = df[["Date", "Cumulative total", "Daily change in cumulative total"]]

    df.loc[:, "Country"] = "United Kingdom"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Source label"] = "Department of Health and Social Care and Public Health England"
    df.loc[:, "Source URL"] = "https://coronavirus.data.gov.uk/developers-guide"
    df.loc[:, "Notes"] = "Sum of tests processed for pillars 1 and 2"
    df.loc[:, "Testing type"] = "PCR only"

    df.to_csv("automated_sheets/United Kingdom - tests.csv", index=False)

if __name__ == '__main__':
    main()



