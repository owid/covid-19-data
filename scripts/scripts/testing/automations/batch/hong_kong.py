import pandas as pd

from cowidev.utils.clean import clean_date_series
from cowidev.testing import CountryTestBase


class HongKong(CountryTestBase):
    location = "Hong Kong"
    units = "tests performed"
    source_label = "Department of Health"
    source_url = "http://www.chp.gov.hk/files/misc/statistics_on_covid_19_testing_cumulative.csv"
    source_url_ref = "http://www.chp.gov.hk/files/misc/statistics_on_covid_19_testing_cumulative.csv"
    rename_columns = {
        "日期由 From Date": "from",
        "日期至 To Date": "Date",
        "檢測數字 Number of tests": "t1",
        "特定群組檢測計劃下的檢測數目 Number of tests under Target Group Testing Scheme": "t2",
        "普及社區檢測計劃下的檢測數目 Number of tests under Universal Community Testing Programme": "t3",
        "臨時檢測中心的檢測數目 Number of tests in Temporary Testing Centres": "t4",
        "社區檢測中心的檢測數目 Number of tests in Community Testing Centres": "t5",
    }

    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.source_url)

    def _load_cases(self):
        url = "http://www.chp.gov.hk/files/misc/latest_situation_of_reported_cases_covid_19_eng.csv"
        df = pd.read_csv(
            url,
            usecols=[
                "As of date",
                "Number of confirmed cases",
                "Number of cases tested positive for SARS-CoV-2 virus",
            ],
        )
        df["Number of confirmed cases"] = df["Number of confirmed cases"].fillna(
            df["Number of cases tested positive for SARS-CoV-2 virus"]
        )
        return df.assign(Date=clean_date_series(df["As of date"], "%d/%m/%Y"))

    def pipe_row_sum(self, df):
        return df.assign(change=df[["t1", "t2", "t3", "t4", "t5"]].sum(axis=1))

    def pipe_date(self, df):
        return df.assign(Date=clean_date_series(df["Date"], "%d/%m/%Y"))

    def pipe_metrics(self, df):
        df = df.groupby("Date", as_index=False).change.sum()
        df = df.sort_values("Date")
        df = df.assign(**{"Cumulative total": df.change.cumsum()})
        return df

    def pipe_pr(self, df: pd.DataFrame) -> pd.DataFrame:
        cases = self._load_cases()
        df = df.merge(cases, on="Date")
        df = df.sort_values("Date")
        cases_over_period = df["Number of confirmed cases"].diff().abs()
        tests_over_period = df["Cumulative total"].diff()
        return df.assign(**{"Positive rate": (cases_over_period / tests_over_period).round(5)})

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_row_sum)
            .pipe(self.pipe_date)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_pr)
            .pipe(self.pipe_metadata)
        )

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df, float_format="%.5f")


def main():
    HongKong().export()


if __name__ == "__main__":
    main()
