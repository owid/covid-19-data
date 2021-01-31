import pandas as pd
import vaxutils


def main():

    url = "https://www.data.gouv.fr/fr/datasets/r/131c6b39-51b5-40a7-beaa-0eafc4b88466"
    df = pd.read_csv(url, sep=",")
    assert df.shape[1] == 4
    assert df.shape[0] == 1

    people_vaccinated = int(df["n_tot_dose1"].values[0])
    people_fully_vaccinated = int(df["n_tot_dose2"].values[0])
    total_vaccinations = people_vaccinated + people_fully_vaccinated
    date = df["jour"].values[0]

    vaxutils.increment(
        location="France",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-personnes-vaccinees-contre-la-covid-19-1/",
        vaccine="Pfizer/BioNTech"
    )


if __name__ == '__main__':
    main()
