import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
import vaxutils


def main():

    url = "https://e.infogram.com/c3bc3569-c86d-48a7-9d4c-377928f102bf"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    for script in soup.find_all("script"):
        if "infographicData" in script.text:
            js_data = json.loads(script.text.replace("window.infographicData=", "")[:-1])
            break
    
    data = js_data["elements"]["content"]["content"]["entities"]["39ac25a9-8af7-4d26-bd19-62a3696920a2"]["props"]["chartData"]["data"][0]

    df = pd.DataFrame(data[1:], columns=data[0])

    only_1dose_people = int(df["Bólusetning hafin"].astype(int).sum())
    people_fully_vaccinated = int(df["Bólusetningu lokið"].astype(int).sum())
    people_vaccinated = only_1dose_people + people_fully_vaccinated
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    date = js_data["updatedAt"][:10]

    vaxutils.increment(
        location="Iceland",
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        date=date,
        source_url="https://www.covid.is/tolulegar-upplysingar-boluefni",
        vaccine="Moderna, Pfizer/BioNTech"
    )


if __name__ == '__main__':
    main()
