import datetime
import pandas as pd
import numpy as np


def get_tests():
    df = pd.read_csv(
        "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Determinaciones.csv",
        usecols=["fecha", "total", "positivos"]
    )

    # Occasional errors where some lab inserts data before 2020
    df = df[df.fecha >= "2020"]
    
    df = df.groupby("fecha", as_index=False).sum()

    df.columns = ["Date", "Daily change in cumulative total", "positive"]

    df["Positive rate"] = (
        df.positive.rolling(7).sum()
        .div(df["Daily change in cumulative total"].rolling(7).sum())
        .round(3)
    )

    df = df.drop(columns=["positive"])

    df["Country"] = "Argentina"
    df["Units"] = "tests performed"
    df["Testing type"] = "PCR only"
    df["Notes"] = np.nan
    df["Source URL"] = "https://datos.gob.ar/dataset/salud-covid-19-determinaciones-registradas-republica-argentina/archivo/salud_0de942d4-d106-4c74-b6b2-3654b0c53a3a"
    df["Source label"] = "Government of Argentina"

    df.to_csv("automated_sheets/Argentina - tests performed.csv", index=False)


# def get_people():
#     df = pd.read_csv(
#         "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Casos.csv",
#         usecols=["fecha_diagnostico", "clasificacion_resumen"]
#     )
#     df = df[-df.fecha_diagnostico.isnull()]

#     # Occasional errors where some lab inserts data before 2020
#     df["fecha_diagnostico"] = df.fecha_diagnostico.str.replace(r"^20[01][0-9]", "2020", regex=True)
    
#     df = (
#         df
#         .groupby("fecha_diagnostico")
#         .size()
#         .to_frame("Daily change in cumulative total")
#         .reset_index()
#         .rename(columns={"fecha_diagnostico": "Date"})
#     )

#     df["Country"] = "Argentina"
#     df["Units"] = "people tested"
#     df["Testing type"] = "PCR only"
#     df["Notes"] = np.nan
#     df["Source URL"] = "https://datos.gob.ar/dataset/salud-covid-19-casos-registrados-republica-argentina/archivo/salud_fd657d02-a33a-498b-a91b-2ef1a68b8d16"
#     df["Source label"] = "Government of Argentina"

#     df.to_csv("automated_sheets/Argentina - people tested.csv", index=False)


if __name__ == '__main__':
    get_tests()
    # get_people()
