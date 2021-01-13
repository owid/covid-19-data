import pandas as pd
import vaxutils


def main():

    url = "https://www.data.gouv.fr/fr/datasets/r/b234a041-b5ea-4954-889b-67e64a25ce0d"
    df = pd.read_csv(url, usecols=["date", "total_vaccines"], sep=";")

    df = df.sort_values("date")

    count = int(df["total_vaccines"].values[-1])
    date = df["date"].values[-1]

    vaxutils.increment(
        location="France",
        total_vaccinations=count,
        date=date,
        source_url="https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-personnes-vaccinees-contre-la-covid-19/",
        vaccine="Pfizer/BioNTech"
    )


if __name__ == '__main__':
    main()
