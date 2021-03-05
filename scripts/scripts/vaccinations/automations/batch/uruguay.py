import pandas as pd

def main():

    df = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/1ktfI1cEm-VyvRbiFkXvzTixrDrCG-85Et9Clz69QBp8/gviz/tq?tqx=out:csv&sheet=Uruguay",
        usecols=["location", "date", "vaccine", "source_url", "total_vaccinations",	"people_vaccinated", "people_fully_vaccinated"]   
    )

    df.to_csv("automations/output/Uruguay.csv", index=False)

if __name__ == "__main__":
    main()
