import pandas as pd


def main():

    url = "https://opendata.digilugu.ee/opendata_covid19_test_county_all.csv"

    df = pd.read_csv(url, usecols=["StatisticsDate", "TotalTests"])

    df = df.groupby("StatisticsDate", as_index=False).sum().sort_values("StatisticsDate")

    df = df.rename(columns={"StatisticsDate": "Date", "TotalTests": "Cumulative total"})

    df.loc[:, "Country"] = "Estonia"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Source URL"] = "https://www.terviseamet.ee/et/koroonaviirus/avaandmed"
    df.loc[:, "Source label"] = "Estonian Health Board"
    df.loc[:, "Notes"] = pd.NA

    df.to_csv("automated_sheets/Estonia.csv", index=False)


if __name__ == '__main__':
    main()
