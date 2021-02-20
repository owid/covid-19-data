import pandas as pd


def main():
    source = "https://github.com/mpanteli/covid19-vaccinations-cyprus/raw/main/data/Cyprus.csv"
    destination = "automations/output/Cyprus.csv"
    pd.read_csv(source).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
