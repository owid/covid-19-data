from collections import defaultdict

import requests
import pandas as pd


metrics_mapping = {
    "COVIDVACAD1": "people_vaccinated",
    "COVIDVACAD2": "people_fully_vaccinated",
    "COVIDVACADT": "total_vaccinations"
}
country_mapping = {
    "PG": "Papua New Guinea",
    "SB": "Solomon Islands",
    "TV": "Tuvalu",
    "TO": "Tonga",
    "WS": "Samoa",
    "NR": "Nauru",
    "FJ": "Fiji"
}

def read(url):
    # Get data
    data = requests.get(url).json()
    return parse_data(data)


def parse_data(data):
    series = data["data"]["dataSets"][0]["series"]
    country_info = parse_country_info(data)
    metrics_info = parse_metrics_info(data)
    date_info = parse_date_info(data)
    vaccination_data = defaultdict(dict)
    for k, v in series.items():
        _, country_idx, metric_idx = k.split(":")
        if metric_idx in metrics_info:
            vaccination_data[country_info[country_idx]][metrics_info[metric_idx]] = build_data_array(v["observations"], date_info)
    return build_df_list(vaccination_data)


def parse_country_info(data):
    # Get country info
    country_info = data["data"]["structure"]["dimensions"]["series"][1]
    if country_info["id"] != "GEO_PICT":
        raise AttributeError("JSON data has changed")
    return {
        str(i): country_mapping[c["id"]] 
        for i, c in enumerate(country_info["values"])
    }


def parse_metrics_info(data):
    # Get metrics info
    metrics_info = data["data"]["structure"]["dimensions"]["series"][2]
    if metrics_info["id"] != "INDICATOR":
        raise AttributeError("JSON data has changed")
    return {
        str(i): metrics_mapping[m["id"]]
        for i, m in enumerate(metrics_info["values"]) if m["id"] in metrics_mapping
    }


def parse_date_info(data):
    # Get date info
    date_info = data["data"]["structure"]["dimensions"]["observation"][0]["values"]
    return {
        str(i): d["name"] for i, d in enumerate(date_info)
    }


def build_data_array(observations, date_info):
    return {
        date_info[k]: v[0] if len(v) == 1 else None 
        for k, v in observations.items()
    }


def build_df_list(data):
    for k, v in data.items():
        data[k] = _build_df(v, k)
    return data
        
def _build_df(dix, country):
    column_metrics = ["people_vaccinated", "total_vaccinations", "people_fully_vaccinated"]
    return (
        pd.DataFrame(dix)
        .dropna(how="all")
        .astype("Int64")
        .drop_duplicates(keep="first")
        .reset_index()
        .rename(columns={"index": "date"})
        .sort_values(by="date")
        .assign(
            location=country,
            source_url="https://www.spc.int/updates/blog/did-you-know/2021/04/stat-of-the-week-covid-19-vaccination-in-the-pacific-island",
            vaccine="Oxford/AstraZeneca"
        )
    )


def main():
    url_country = "+".join(country_mapping.keys())
    url = (
        f"https://stats-nsi-stable.pacificdata.org/rest/data/SPC,DF_COVID_VACCINATION,1.0/D.{url_country}.?"
        "startPeriod=2021-02-02&format=jsondata"
    )
    data = read(url)
    for c, df in data.items():
        df.to_csv(f"output/{c}.csv", index=False)


if __name__ == "__main__":
    main()
