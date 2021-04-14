import pandas as pd


class Ecuador(object):

    def __init__(self, source_url: str, location: str, columns_rename: dict = None):
        """Constructor

        Args:
            source_url (str): Source data url
            location (str): Location name
            columns_rename (dict, optional): Maps original to new names. Defaults to None.
        """
        self.source_url = source_url
        self.location = location
        self.columns_rename = columns_rename
    
    @property
    def output_file(self):
        return f"automations/output/{self.location}.csv"

    def read(self) -> pd.DataFrame:
        url = f"{self.source_url}/raw/master/datos_crudos/vacunas/vacunas.csv"
        return pd.read_csv(url)

    def check_columns(self, df: pd.DataFrame, expected) -> pd.DataFrame:
        n_columns = df.shape[1]
        if n_columns != expected:
            raise ValueError(f"The provided input does not have {expected} columns. It has n_columns columns")
        return df

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.columns_rename:
            return df.rename(columns=self.columns_rename)
        return df

    def format_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=pd.to_datetime(df.date, format="%d/%m/%Y").dt.strftime("%Y-%m-%d"))

    def enrich_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            source_url=self.source_url,
        )

    def enrich_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        """TODO: https://github.com/andrab/ecuacovid/blob/master/datos_crudos/vacunas/fabricantes.csv"""
        def _enrich_vaccine(date: str):
            if date < '2021-03-06':
                return 'Pfizer/BioNTech'
            elif date < '17/03/2021':
                return "Pfizer/BioNTech, Sinovac"
            else:
                return "Pfizer/BioNTech, Oxford/AstraZeneca, Sinovac" 
        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.check_columns, expected=4)
            .pipe(self.rename_columns)
            .pipe(self.format_date)
            .pipe(self.enrich_columns)
            .pipe(self.enrich_vaccine)
        )

    def to_csv(self, output_file: str = None):
        """Generalized"""
        df = self.read().pipe(self.pipeline)
        if output_file is None:
            output_file = self.output_file
        df.to_csv(output_file, index=False)


def main():
    Ecuador(
        source_url="https://github.com/andrab/ecuacovid",
        location="Ecuador",
        columns_rename={
            "fecha": "date",
            "dosis_total": "total_vaccinations",
            "primera_dosis": "people_vaccinated",
            "segunda_dosis": "people_fully_vaccinated",
        }
    ).to_csv()


if __name__ == "__main__":
    main()
