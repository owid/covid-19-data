from bs4 import BeautifulSoup
import requests, pytesseract, pytz, sys
import pandas as pd
from datetime import datetime
try:
    from PIL import Image
except ImportError:
    import Image

def getImageURL(hostname, path):
    url = hostname + path
    url_request = requests.get(url)
    soup = BeautifulSoup(url_request.text, features="lxml")
    return hostname + soup.find("div", {"class": "interSec1Lchikun left"}).find('img')['src']

def getImgText(imgURL):
    imgText = pytesseract.image_to_string(
        Image.open(
            requests.get(imgURL, stream=True).raw)
    )
    return imgText

def main():
    # Obtains both Image URL and text in Image via tesseract.
    hostname, path = "https://www.minsalud.gov.co", "/salud/publica/Vacunacion/Paginas/Vacunacion-covid-19.aspx"
    imgURL = getImageURL(hostname, path)
    imgText = getImgText(imgURL)

    # Gets the applied doses number 
    b = imgText.find("DOSIS APLICADAS:")
    appliedDoses = imgText[(b + 17):(b + 27)]

    # Eliminates all the involved strings if there's any
    new_vaccinations = int(re.sub("[^\d]", "", appliedDoses))

    # Open CSV file
    df = pd.read_csv('Colombia.csv')
    # print(df)
    old_vaccinations = df['total_vaccinations'][df.index.stop - 1]

    # Check if the obtained vaccinations are the same as before
    if new_vaccinations == old_vaccinations:
        print("Obtained vaccinations already in csv file. Please try again later.")
        sys.exit()
    
    # Process data
    date = datetime.now(pytz.timezone("America/Bogota")).date().strftime("%Y-%m-%d")
    newRow = {
        "location": "Colombia",
        "date": date,
        "vaccine": "Pfizer/BioNTech, Sinovac",
        "source_url": hostname + path,
        "total_vaccinations": new_vaccinations,
        "people_vaccinated": new_vaccinations
        # People fully vaccinated hasn't been counted... yet.
    }

    # Adds row
    df = df.append(newRow, ignore_index=True)
    # print(df)
    df.to_csv("automations/output/Colombia.csv", index=False)

    # print(new_vaccinations)

if __name__ == '__main__':
    main()
