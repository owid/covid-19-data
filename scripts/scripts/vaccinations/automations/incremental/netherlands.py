import json
import requests
import pandas as pd
import vaxutils


def main():

    url = "https://raw.githubusercontent.com/minvws/nl-covid19-data-dashboard/develop/packages/app/src/locale/nl.json"
    data = json.loads(requests.get(url).content)

    total_vaccinations = int(data["vaccinaties"]["data"]["sidebar"]["last_value"]["total_vaccinated"])

    date = data["vaccinaties"]["data"]["sidebar"]["last_value"]["date_unix"]
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
