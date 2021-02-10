import pandas as pd


def read(source: str) -> pd.DataFrame:
    source = "https://github.com/juancri/covid19-vaccination/raw/master/output/chile-vaccination.csv"
    return pd.read_csv(source)


def filter_totals(input: pd.DataFrame) -> pd.DataFrame:
    return input[input.Region == "Total"]


def transpose(input: pd.DataFrame) -> pd.DataFrame:
    """The dataset is provided with new entries as new columns.
    It’s more standard to deal with new entries as new rows.
    """
    return (
        input.T.drop(["Region", "Dose"])
        .astype(int)
        .reset_index()
        .rename(
            columns={
                "index": "date",
                0: "people_vaccinated",
                1: "people_fully_vaccinated",
            }
        )
        .groupby(by=["people_vaccinated", "people_fully_vaccinated"])
        .min()
        .reset_index()
    )


def select_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input[
        [
            "location",
            "date",
            "people_vaccinated",
            "people_fully_vaccinated",
            "total_vaccinations",
            "vaccine",
            "source_url",
        ]
    ]


def enrich_vaccine_name(input: pd.DataFrame) -> pd.DataFrame:
    """Return vaccine names.

    Add logic here if different vaccines were in use in different periods.

    Args:
        input (pandas.DataFrame): Data. Must contain a column named 'date'
          with date information.

    Returns:
        A data frame with a column for the vaccinations.
    """

    def _enrich_vaccine_name(date: str) -> str:
        if date >= "2021-02-03":
            return "Pfizer/BioNTech, Sinovac"
        return "Pfizer/BioNTech"

    return input.assign(vaccine=input.date.apply(_enrich_vaccine_name))


def enrich_total_vaccinations(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        total_vaccinations=input.people_vaccinated + input.people_fully_vaccinated
    )


def enrich_metadata(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        source_url=(
            "https://informesdeis.minsal.cl/SASVisualAnalytics/"
            "?reportUri=%2Freports%2Freports%2F1a8cc7ff-7df0-474f-a147-929ee45d1900"
            "&sectionIndex=0"
            "&sso_guest=true"
            "&reportViewOnly=true"
            "&reportContextBar=false"
            "&sas-welcome=false"
        ),
        location="Chile",
    )


def replace_zeroes_with_nans(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        people_fully_vaccinated=input.people_fully_vaccinated.replace({0: pd.NA})
    )


def preprocess(input: pd.DataFrame) -> pd.DataFrame:
    return input.pipe(filter_totals).pipe(transpose)


def enrich(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(enrich_vaccine_name)
        .pipe(enrich_total_vaccinations)
        .pipe(enrich_metadata)
    )


def post_process(input: pd.DataFrame) -> pd.DataFrame:
    return input.pipe(replace_zeroes_with_nans).pipe(select_columns)


def pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return input.pipe(preprocess).pipe(enrich).pipe(post_process)


def main():
    source = "https://github.com/juancri/covid19-vaccination/raw/master/output/chile-vaccination.csv"
    destination = "automations/output/Chile.csv"
    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
