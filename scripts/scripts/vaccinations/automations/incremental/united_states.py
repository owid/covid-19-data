import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import vax_utils

def main():

    url = "https://www.cdc.gov/coronavirus/2019-ncov/vaccines/index.html"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    
    count = soup.find("p", text=re.compile(r"ADMINISTERED DOSES")).parent.findChildren()[1].text
    count = vax_utils.clean_count(count)

    date = str(soup.find_all(text=re.compile(r"Update:.*202\d"))[0])
    date = re.search(r"[A-Za-z]+ \d+ 202\d", date).group(0)
    date = pd.to_datetime(date)
    date = str(date.date())

    vax_utils.increment(
        location = "United States",
        total_vaccinations = count,
        date = date,
        source_url = url,
        vaccine = "Pfizer/BioNTech"
    )

if __name__ == "__main__":
    main()
