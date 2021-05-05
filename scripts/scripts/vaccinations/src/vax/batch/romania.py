import os

import requests
import pandas as pd


class Romania:

    def __init__(self, source_url: str, source_url_ref: str,location: str, vaccine_mapping: dict,
                 vaccines_1d: list, columns_rename: dict = None):
        self.source_url = source_url
        self.source_url_ref = source_url_ref
        self.location = location
        self.columns_rename = columns_rename
        self.vaccine_mapping = vaccine_mapping
        self.vaccines_1d = vaccines_1d

    @property
    def output_file(self):
        return f"./output/{self.location}.csv"

    @property
    def output_file_manufacturer(self):
        return os.path.join("output", "by_manufacturer", f"{self.location}.csv")

    def read(self) -> pd.DataFrame:
        data = requests.get(self.source_url).json()
        return (
            pd.DataFrame.from_dict(
                data["historicalData"],
                orient="index",
                columns=["vaccines", "numberTotalDosesAdministered"]
            )
            .reset_index()
            .dropna()
            .sort_values(by="index")
        )

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.columns_rename)

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def _unnest_vaccine_details(self, df: pd.DataFrame) -> pd.DataFrame:
        def _doses_by_vax(x):
            return {k: v['total_administered'] for k, v in x.items()}
        df_vax = pd.DataFrame.from_records(df.vaccines.apply(_doses_by_vax), index=df.index)
        # Check vaccine names - Any new ones?
        vaccines_unknown = set(df_vax.columns).difference(self.vaccine_mapping)
        if vaccines_unknown:
            raise ValueError(f"Unrecognized vaccine {vaccines_unknown}")
        df_vax.columns = [self.vaccine_mapping[col] for col in df_vax.columns]
        return df_vax.merge(df, left_index=True, right_index=True)

    def pipeline_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_location)
            .pipe(self._unnest_vaccine_details)
        )

    def pipe_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url_ref)

    def pipe_people_fully_vaccinated(self, df: pd.DataFrame) -> pd.DataFrame:
        def _people_fully_vaccinated(x):
            return sum(v["immunized"] for v in x.values())
        return df.assign(
            people_fully_vaccinated=df.vaccines.apply(_people_fully_vaccinated).cumsum()
        )

    def pipe_people_vaccinated(self, df: pd.DataFrame) -> pd.DataFrame:
        def _people_fully_vaccinated_1d(x):
            return sum(v['immunized'] for k, v in x.items() if k in self.vaccines_1d)
        people_fully_vaccinated_1d=df.vaccines.apply(_people_fully_vaccinated_1d).cumsum()
        return df.assign(
            people_vaccinated=(
                df.total_vaccinations - df.people_fully_vaccinated + people_fully_vaccinated_1d
            )
        )

    def _vaccine_start_dates(self, df: pd.DataFrame):
        date2vax = sorted((
            (
                df.loc[df[vaccine] > 0, "date"].min(),
                vaccine
            )
            for vaccine in self.vaccine_mapping.values()
        ), key=lambda x: x[0], reverse=True)
        return [
            (
                date2vax[i][0],
                ", ".join(sorted(v[1] for v in date2vax[i:]))
            )
            for i in range(len(date2vax))
        ]

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        vax_date_mapping = self._vaccine_start_dates(df)
        def _enrich_vaccine(date: str) -> str:
            for dt, vaccines in vax_date_mapping:
                if date >= dt:
                    return vaccines
            raise ValueError(f"Invalid date {date} in DataFrame!")
        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def pipe_select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[[
            "date",
            "location",
            "vaccine",
            "source_url",
            "total_vaccinations",
            "people_vaccinated",
            "people_fully_vaccinated"
        ]]

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Could be generalized."""
        return (
            df
            .pipe(self.pipe_source)
            .pipe(self.pipe_people_fully_vaccinated)
            .pipe(self.pipe_people_vaccinated)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_select_output_columns)
        )

    def pipe_manufacturer_melt(self, df: pd.DataFrame) -> pd.DataFrame:
        id_vars = ["date", "location"]
        df = df[id_vars + list(self.vaccine_mapping.values())].melt(
            id_vars=id_vars,
            var_name="vaccine",
            value_name="total_vaccinations"
        )
        return df[df.total_vaccinations != 0]

    def pipe_manufacturer_cumsum(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(total_vaccinations=df.groupby("vaccine", as_index=False).cumsum())

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_manufacturer_melt)
            .pipe(self.pipe_manufacturer_cumsum)
        )

    def to_csv(self, output_file: str = None, output_file_manufacturer: str = None):
        df_base = self.read().pipe(self.pipeline_base)
        # Export data
        df = df_base.copy().pipe(self.pipeline)
        if output_file is None:
            output_file = self.output_file
        df.to_csv(output_file, index=False)
        # Export manufacturer data
        df = df_base.copy().pipe(self.pipeline_manufacturer)
        if output_file_manufacturer is None:
            output_file_manufacturer = self.output_file_manufacturer
        df.to_csv(output_file_manufacturer, index=False)

def main():
    Romania(
        source_url="https://d35p9e4fm9h3wo.cloudfront.net/latestData.json",
        source_url_ref="https://datelazi.ro/",
        location="Romania",
        columns_rename={
            "index": "date",
            "numberTotalDosesAdministered": "total_vaccinations"
        },
        vaccine_mapping = {
            "pfizer": "Pfizer/BioNTech",
            "moderna": "Moderna",
            "astra_zeneca": "Oxford/AstraZeneca",
            "johnson_and_johnson": "Johnson&Johnson",
        },
        vaccines_1d=['johnson_and_johnson']
    ).to_csv()


if __name__ == "__main__":
    main()
