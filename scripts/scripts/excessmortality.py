import requests
import os
import datetime
import pandas as pd


CURRENT_DIR = os.path.dirname(__file__)
OUTPUT_PATH = os.path.join(CURRENT_DIR, '../../public/data/excess_mortality/')


def update_dataset():

    df = pd.read_csv("https://github.com/owid/owid-datasets/raw/master/datasets/Excess%20Mortality%20Data%20%E2%80%93%20OWID%20(2021)/Excess%20Mortality%20Data%20%E2%80%93%20OWID%20(2021).csv")
    
    df = df.rename(columns={
        "Entity": "location",
        "Year": "date",
        "Excess mortality P-scores, all ages": "p_scores_all_ages",
        "Excess mortality P-scores, ages 0–14": "p_scores_0_14",
        "Excess mortality P-scores, ages 15–64": "p_scores_15_64",
        "Excess mortality P-scores, ages 65–74": "p_scores_65_74",
        "Excess mortality P-scores, ages 75–84": "p_scores_75_84",
        "Excess mortality P-scores, ages 85+": "p_scores_85plus",
        "Deaths, 2020, all ages": "deaths_2020_all_ages",
        "Average deaths, 2015–2019, all ages": "average_deaths_2015_2019_all_ages"
    })

    df.loc[:, "date"] = [pd.to_datetime("2020-01-01") + datetime.timedelta(days=d) for d in df.date]
    df = df.sort_values(["location", "date"])

    df.to_csv(os.path.join(OUTPUT_PATH, "excess_mortality.csv"), index=False)


# def update_readme():
    
#     response = requests.get("https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/Excess%20Mortality%20Data%20%E2%80%93%20HMD%20(2021)/README.md")
#     assert response.status_code == 200
#     with open(os.path.join(OUTPUT_PATH, "README.md"), "wb") as file:
#         file.write(response.content)


if __name__ == "__main__":
    update_dataset()
    # update_readme()
