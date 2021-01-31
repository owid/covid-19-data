import json
import requests
import pandas as pd
import vaxutils


def main():

    url = "https://coronadashboard.government.nl/_next/data/cs_HykNUpz70XEV85dq0b/landelijk/vaccinaties.json"
    data = json.loads(requests.get(url).content)

    assert data["pageProps"]["text"]["vaccinaties"]["data"]["kpi_total"]["title"] == "Number of doses administered"

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
