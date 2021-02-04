import json
import re
import requests
import dateparser
import vaxutils


def main():

    url = "https://corona.fo/json/stats"
    data = json.loads(requests.get(url).content)

    people_vaccinated = int(data["stats"][0]["first_vaccine_number"])
    people_fully_vaccinated = int(data["stats"][0]["second_vaccine_number"])
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    date = data["stats"][0]["vaccine_last_update"]
    date = re.search(r"\d+\. \w+", date).group(0)
    date = str(dateparser.parse(date, languages=["da"]).date())

    vaxutils.increment(
        location="Faeroe Islands",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="https://corona.fo/api",
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
