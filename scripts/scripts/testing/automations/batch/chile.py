import pandas as pd


def main():

    tests = pd.read_csv("https://github.com/MinCiencia/Datos-COVID19/raw/master/output/producto17/PCREstablecimiento_std.csv")
    tests = tests[tests["Establecimiento"] == "Total realizados"]
    tests = tests[["fecha", "Numero de PCR"]].rename(columns={
        "fecha": "Date", "Numero de PCR": "Cumulative total"
    })
    tests = tests.dropna()

    pr = pd.read_csv("https://github.com/MinCiencia/Datos-COVID19/raw/master/output/producto55/Positividad_nacional.csv")
    pr = pr.dropna()
    pr["positividad"] = pr["positividad"].round(3)
    pr = pr.rename(columns={"fecha": "Date", "positividad": "Positive rate"})

    df = pd.merge(tests, pr, on="Date", how="outer").sort_values("Date")

    df.loc[:, "Country"] = "Chile"
    df.loc[:, "Source URL"] = "https://github.com/MinCiencia/Datos-COVID19"
    df.loc[:, "Source label"] = "Ministry of Science, Technology, Knowledge and Innovation"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Notes"] = pd.NA
    df.loc[:, "Testing type"] = "PCR only"

    df.to_csv("automated_sheets/Chile.csv", index=False)


if __name__ == '__main__':
    main()
