from bs4 import BeautifulSoup
import pytz, sys, re
import requests, imutils, cv2, pytesseract
from pytesseract import image_to_string as getImgText
import numpy as np
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

def processImg(url):
    img = imutils.url_to_image(url)
    img = img[299:389, 105:616] # crops the image to a specific position [y1:y2, x1:x2]
                                # in this case, it's cropping the image to the amount of applied vaccines position
    img_gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
    img_gray, img_bin = cv2.threshold(img_gray,128,255,cv2.THRESH_BINARY | cv2.THRESH_OTSU)
    img_gray = cv2.bitwise_not(img_bin)
    kernel = np.ones((2, 1), np.uint8)
    img = cv2.erode(img_gray, kernel, iterations=1)
    img = cv2.dilate(img, kernel, iterations=1)
    # credit to hucker marius for this portion of code. 
    # this literally saved my life since imutils' tools weren't working the way i expected
    # https://towardsdatascience.com/optical-character-recognition-ocr-with-less-than-12-lines-of-code-using-python-48404218cccb
    # cv2.imwrite("img.jpeg", img)
    return getImgText(img)

def main():
    # Obtains both Image URL and text in Image via tesseract.
    hostname, path = "https://www.minsalud.gov.co", "/salud/publica/Vacunacion/Paginas/Vacunacion-covid-19.aspx"
    imgURL = getImageURL(hostname, path)

    # Process Image
    imgText = processImg(imgURL)

    # Eliminates all the involved strings if there's any
    # print(imgText)
    new_vaccinations = int(re.sub("[^\d]", "", imgText))
    # print("Vaccinated: %i" % (new_vaccinations))

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
