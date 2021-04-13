import json
import requests
import pandas as pd
from datetime import date
import numpy as np


def main():

    ##Antigen
    url = 'https://atlas.jifo.co/api/connectors/425b93dc-c055-477c-b81a-5d4d9a1275f7'
    response = requests.get(url).text
    nested_dict = json.loads(response)

    ag = pd.DataFrame(nested_dict["data"][4])

    new_header = ag.iloc[0]
    ag = ag[1:]
    ag.columns = new_header

    ag["Date"] = pd.to_datetime(ag[""], dayfirst=True)
    del ag[""]

    ag["Positivas"] = ag["Positivas"].str.replace(',','').replace('','0').astype(float)
    ag["Total Px Ag"] = ag["Total Px Ag"].str.replace(',','').replace('','0').astype(float)

    ##PCR
    pcr = pd.read_csv(
        'https://www.datos.gov.co/resource/8835-5baf.csv',
        usecols=['fecha', 'positivas_acumuladas','negativas_acumuladas']
    )

    pcr = pcr[(pcr["fecha"] != "Acumulado Feb") & (pcr.fecha.notnull())]
    pcr["Date"] = pd.to_datetime(pcr["fecha"], dayfirst=True)
    del pcr["fecha"]

    ##Combine
    df = pd.merge(ag, pcr, how='outer').sort_values(by="Date")
      
    df["Cumulative total"] = (
        df["positivas_acumuladas"].fillna(0)
        .add(df["negativas_acumuladas"].fillna(0))
        .add(df["Total Px Ag"].fillna(0))
    )

    df["Positive total"] = (
        df["positivas_acumuladas"].fillna(0)
        .add(df["Positivas"].fillna(0))
    )

    df = df[df["Cumulative total"] != 0]
    df = df[df["Cumulative total"] > df["Cumulative total"].shift(1)]
    df = df[df["Positive total"] != 0]

    df["Positive rate"] = (
        ((df["Positive total"] - df["Positive total"].shift(1)).rolling(7).mean())
        .div((df["Cumulative total"] - df["Cumulative total"].shift(1)).rolling(7).mean())
        .round(3)
    )

    df = df.drop(columns=[
        "positivas_acumuladas", "negativas_acumuladas", "Positivas", "Total Px Ag", "Positive total"
    ])

    df["Country"] = "Colombia"
    df["Units"] = "tests performed"
    df["Source URL"] = "https://www.ins.gov.co/Noticias/Paginas/coronavirus-pcr.aspx"
    df["Source label"] = "National Institute of Health"
    df["Notes"] = np.nan
    df.reset_index(drop=True, inplace=True)

    df.to_csv("automated_sheets/Colombia.csv", index=False)


if __name__ == '__main__':
    main()
