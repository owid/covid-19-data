import datetime
import json
import pytz
import requests
import vaxutils


def main():

    url = "https://covid19asi.saglik.gov.tr/covid19asisayisi?getir=asiYapilanKisiSayisi"
    data = json.loads(requests.get(url).content)

    count = data["asisayisi"]

    date = str(datetime.datetime.now(pytz.timezone("Asia/Istanbul")).date())

    vaxutils.increment(
        location="Turkey",
        total_vaccinations=count,
        date=date,
        source_url="https://covid19asi.saglik.gov.tr/",
        vaccine="Sinovac"
    )


if __name__ == '__main__':
    main()
