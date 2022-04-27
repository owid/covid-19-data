from datetime import datetime
from itertools import chain

import pandas as pd


METRICS = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "total_boosters"]

VACCINES_ACCEPTED = [
    "Abdala",
    "CanSino",
    "Covaxin",
    "COVIran Barekat",
    "EpiVacCorona",
    "FAKHRAVAC",
    "IMBCAMS",
    "Johnson&Johnson",
    "KCONVAC",
    "Medicago",
    "Medigen",
    "Moderna",
    "Novavax",
    "Oxford/AstraZeneca",
    "Pfizer/BioNTech",
    "QazVac",
    "Razi Cov Pars",
    "Sinopharm/Beijing",
    "Sinopharm/Wuhan",
    "Sinovac",
    "Soberana02",
    "Soberana Plus",
    "SpikoGen",
    "Sputnik Light",
    "Sputnik V",
    "Turkovac",
    "ZF2001",
    "ZyCoV-D",
]

VACCINES_ONE_DOSE = [
    "Johnson&Johnson",
    "CanSino",
    "Sputnik Light",
    "Soberana Plus",
]

VACCINES_THREE_DOSES = [
    "ZF2001",
    "Abdala",
    "Razi Cov Pars",  # 3rd dose (intranasal spray) not reported in Iran as a 3rd dose
    "ZyCoV-D",
]


def country_df_sanity_checks(
    df: pd.DataFrame,
    monotonic_check_skip: list = [],
    anomalies: bool = True,
    anomaly_check_skip: list = [],
) -> pd.DataFrame:
    checker = CountryChecker(
        df,
        monotonic_check_skip=monotonic_check_skip,
        anomalies=anomalies,
        anomaly_check_skip=anomaly_check_skip,
    )
    checker.run()


class CountryChecker:
    def __init__(
        self,
        df: pd.DataFrame,
        allow_extra_cols: bool = True,
        monotonic_check_skip: list = [],
        anomalies: bool = True,
        anomaly_check_skip: list = [],
    ):
        self.location = self._get_location(df)
        self.df = df
        self.allow_extra_cols = allow_extra_cols
        self.skip_monocheck_ids = self._skip_check_ids(monotonic_check_skip)
        self.anomalies = anomalies
        self.skip_anomalcheck_ids = self._skip_check_ids(anomaly_check_skip)

    def _get_location(self, df):
        x = df.loc[:, "location"].unique()
        if len(x) != 1:
            locations = df.loc[:, "location"].unique()
            raise ValueError(f"More than one location found: {locations}")
        return x[0]

    def _skip_check_ids(self, check_skip):
        def _f(x):
            dt = x["date"].strftime("%Y%m%d")
            if isinstance(x["metrics"], list):
                return [dt + m for m in x["metrics"]]
            return [x["date"].strftime("%Y%m%d") + x["metrics"]]

        res = [_f(x) for x in check_skip]
        return list(chain.from_iterable(res))

    @property
    def metrics_present(self):
        cols = ["total_vaccinations"]
        if "people_vaccinated" in self.df.columns:
            cols.append("people_vaccinated")
        if "people_fully_vaccinated" in self.df.columns:
            cols.append("people_fully_vaccinated")
        if "total_boosters" in self.df.columns:
            cols.append("total_boosters")
        return cols

    def check_column_names(self):
        cols = ["total_vaccinations", "vaccine", "date", "location", "source_url"]
        cols_extra = cols + [
            "people_vaccinated",
            "people_fully_vaccinated",
            "total_boosters",
        ]
        cols_missing = [col for col in cols if col not in self.df.columns]
        if cols_missing:
            raise ValueError(f"{self.location} -- df missing column(s): {cols_missing}.")
        # Ensure validity of column names in df
        if not self.allow_extra_cols:
            cols_wrong = [col for col in self.df.columns if col not in cols_extra]
            if cols_wrong:
                raise ValueError(f"{self.location} -- df contains invalid column(s): {cols_wrong}.")

    def check_source_url(self):
        if self.df.source_url.isnull().any():
            raise ValueError(f"{self.location} -- Invalid source_url! NaN values found.")

    def check_vaccine(self):
        if self.df.vaccine.isnull().any():
            raise ValueError(f"{self.location} -- Invalid vaccine! NaN values found.")
        vaccines_used = set([xx for x in self.df.vaccine.tolist() for xx in x.split(", ")])
        if not all([vac in VACCINES_ACCEPTED for vac in vaccines_used]):
            vaccines_wrong = [vac for vac in vaccines_used if vac not in VACCINES_ACCEPTED]
            raise ValueError(f"{self.location} -- Invalid vaccine detected! Check {vaccines_wrong}.")

    def check_date(self):
        if self.df.date.isnull().any():
            raise ValueError(f"{self.location} -- Invalid dates! NaN values found.")
        if self.df.date.min() < datetime(2020, 12, 1):
            raise ValueError(f"{self.location} -- Invalid dates! Check {self.df.date.min()}")
        ds = self.df.date.value_counts()
        dates_wrong = ds[ds > 1].index
        msk = self.df.date.isin(dates_wrong)
        if not self.df[msk].empty:
            raise ValueError(f"{self.location} -- Check `date` field, there are duplicates: {self.df[msk]}")

    def check_location(self):
        if self.df.location.isnull().any():
            raise ValueError(f"{self.location} -- Invalid location! NaN values found. Check {self.df.location}.")
        if self.df.location.nunique() != 1:
            raise ValueError(
                f"{self.location} -- Invalid location! More than one location found. Check {self.df.location}."
            )

    def check_metrics(self):
        df = self.df.sort_values(by="date")  # [self.metrics_present]
        # Monotonically
        self._check_metrics_monotonic(df)
        # Inequalities
        self._check_metrics_inequalities(df)
        # Anomalies
        if self.anomalies:
            self._check_metrics_anomalies(df)

    def _check_metrics_monotonic(self, df: pd.DataFrame):
        # Use info from monotonic_check_skip to raise exception or not
        for col in self.metrics_present:
            _x = df.dropna(subset=[col])
            if not _x[col].is_monotonic:
                idx_wrong = _x[col].diff() < 0
                wrong_rows = _x.loc[idx_wrong]
                wrong_ids = wrong_rows.date.dt.strftime("%Y%m%d") + col
                if not wrong_ids.isin(self.skip_monocheck_ids).all():
                    raise ValueError(
                        f"{self.location} -- Column {col} must be monotonically increasing! Check:\n{wrong_rows}"
                    )

    def _check_metrics_inequalities(self, df: pd.DataFrame):
        if ("total_vaccinations" in df.columns) and ("people_vaccinated" in df.columns):
            df_ = df[["people_vaccinated", "total_vaccinations"]].dropna().copy()
            msk = df_["total_vaccinations"] < df_["people_vaccinated"]
            if (msk).any():
                raise ValueError(
                    f"{self.location} -- total_vaccinations can't be < people_vaccinated!\n{df.loc[msk[msk].index]}"
                )
        if ("total_vaccinations" in df.columns) and ("people_fully_vaccinated" in df.columns):
            df_ = df[["people_fully_vaccinated", "total_vaccinations"]].dropna().copy()
            if (df_["total_vaccinations"] < df_["people_fully_vaccinated"]).any():
                raise ValueError(f"{self.location} -- total_vaccinations can't be < people_fully_vaccinated!")
        if ("total_vaccinations" in df.columns) and ("total_boosters" in df.columns):
            df_ = df[["total_boosters", "total_vaccinations"]].dropna().copy()
            if (df_["total_vaccinations"] < df_["total_boosters"]).any():
                raise ValueError(f"{self.location} -- total_vaccinations can't be < total_boosters!")
        if ("people_vaccinated" in df.columns) and ("people_fully_vaccinated" in df.columns):
            df_ = df[["people_vaccinated", "people_fully_vaccinated"]].dropna().copy()
            msk = df_["people_vaccinated"] < df_["people_fully_vaccinated"]
            if (msk).any():
                raise ValueError(
                    f"{self.location} -- people_vaccinated can't be <"
                    f" people_fully_vaccinated!\n{df.loc[msk[msk].index]}"
                )

    def _check_metrics_anomalies(self, df):
        for metric in self.metrics_present:
            self._check_anomalies(df, metric)

    def _check_anomalies(self, df, metric, th=6):
        # Get metric values above 10,000
        df_ = df.set_index("date")
        df_metric = df_.loc[(df_[metric] > 10000), metric]
        # Compute rolling average, 7 days. NaNs are filled with non-smoothed values
        window_size = "7d"
        m = df_metric.rolling(window_size, min_periods=2).mean().shift(1)
        m.loc[m.isnull()] = df_metric[m.isnull()]
        # Compute ratio between rolling average and value. Build Anomalies dataframe
        t = df_metric / (m + 1e-9)
        t = pd.DataFrame(
            {
                f"{metric}_{window_size}": m[t > th],
                f"{metric}_ratio": t[t > th],
            }
        )
        anomalies = df_.loc[t.index, [metric]].merge(t, on="date").reset_index()
        if not anomalies.empty:
            wrong_ids = anomalies.date.dt.strftime("%Y%m%d") + metric
            if not wrong_ids.isin(self.skip_anomalcheck_ids).all():
                raise ValueError(f"{self.location} -- Potential anomalies found ⚠️:\n{anomalies}")

    def run(self):
        # Ensure required columns are present
        self.check_column_names()
        # Source url consistency
        self.check_source_url()
        # Vaccine consistency
        self.check_vaccine()
        # Date consistency
        self.check_date()
        # Location consistency
        self.check_location()
        # Metrics checks
        self.check_metrics()


def validate_vaccines(df, vaccines_accepted, vaccines_raw=None):
    if vaccines_raw != None:
        vaccines_wrong = set(vaccines_raw).difference(vaccines_accepted)
    else:
        vaccines_wrong = set(df["vaccine"].unique()).difference(vaccines_accepted)
    if vaccines_wrong:
        raise ValueError(f"Missing vaccines: {vaccines_wrong}")
