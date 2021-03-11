import datetime
import re
import sys
import requests

import imutils
import cv2
import pytesseract
from bs4 import BeautifulSoup
import pytz
from pytesseract import image_to_string as getImgText
import numpy as np
import pandas as pd
try:
    from PIL import Image
except ImportError:
    import Image

import vaxutils


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

    data = pd.Series({
        "location": "Colombia",
        "date": str(datetime.datetime.now(pytz.timezone("America/Bogota")).date() - datetime.timedelta(days=1)),
        "source_url": "https://www.minsalud.gov.co/salud/publica/Vacunacion/Paginas/Vacunacion-covid-19.aspx",
        "vaccine": "Pfizer/BioNTech, Sinovac",
    })

    # Obtains both Image URL and text in Image via tesseract.
    hostname, path = "https://www.minsalud.gov.co", "/salud/publica/Vacunacion/Paginas/Vacunacion-covid-19.aspx"
    imgURL = getImageURL(hostname, path)

    # Process Image
    imgText = processImg(imgURL)

    # Eliminates all the involved strings if there's any
    # print(imgText)
    total_vaccinations = int(re.sub(r"[^\d]", "", imgText))
    # print("Vaccinated: %i" % (total_vaccinations))
    data["total_vaccinations"] = total_vaccinations

    vaxutils.increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == '__main__':
    main()
