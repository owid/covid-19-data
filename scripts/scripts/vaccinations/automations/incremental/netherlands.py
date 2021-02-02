import json
import requests
import pandas as pd
import vaxutils


def main():

    url = "https://raw.githubusercontent.com/minvws/nl-covid19-data-dashboard/master/packages/app/src/locale/en.json"
    data = json.loads(requests.get(url).content)

    assert data["vaccinaties"]["data"]["kpi_total"]["title"] == "Number of doses administered"

    total_vaccinations = int(data["vaccinaties"]["data"]["kpi_total"]["value"])

    date = data["vaccinaties"]["data"]["kpi_total"]["date_of_report_unix"]
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
