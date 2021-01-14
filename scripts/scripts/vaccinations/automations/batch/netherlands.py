import pandas as pd

def main():

    df = pd.read_csv("https://github.com/YorickBleijenberg/COVID_data_RIVM_Netherlands/raw/master/vaccination/people.vaccinated.csv")
    df.loc[:, "location"] = "Netherlands"
    df = df.sort_values("date").groupby("total_vaccinations", as_index=False).head(1)
    df.to_csv("automations/output/Netherlands.csv", index=False)

if __name__ == '__main__':
    main()
