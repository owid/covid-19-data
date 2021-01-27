import json
import requests
import pandas as pd
import vaxutils


def main():

    url = "https://coronadashboard.government.nl/_next/data/5jBzTJhaZyTn2juMr4Cna/index.json"
    data = json.loads(requests.get(url).content)

    total_vaccinations = int(data["pageProps"]["text"]["vaccinaties"]["data"]["kpi_total"]["value"])

    date = data["pageProps"]["text"]["vaccinaties"]["data"]["kpi_total"]["date_of_report_unix"]
    date = str(pd.to_datetime(date, unit="s").date())

    vaxutils.increment(
        location="Netherlands",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url="https://coronadashboard.rijksoverheid.nl/landelijk/vaccinaties",
        vaccine="Moderna, Pfizer/BioNTech"
    )


if __name__ == '__main__':
    main()
