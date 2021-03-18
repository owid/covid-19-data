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
    # Scrapes for the image asset
    url = hostname + path
    url_request = requests.get(url)
    soup = BeautifulSoup(url_request.text, features="lxml")
    return hostname + soup.find("div", {"class": "interSec1Lchikun left"}).find('img')['src']


def processImg(img):
    img_gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
    img_gray, img_bin = cv2.threshold(img_gray,128,255,cv2.THRESH_BINARY | cv2.THRESH_OTSU)
    img_gray = cv2.bitwise_not(img_bin)
    kernel = np.ones((2, 1), np.uint8)
    img = cv2.erode(img_gray, kernel, iterations=1)
    img = cv2.dilate(img, kernel, iterations=1)
    # credit to hucker marius for this portion of code. 
    # this literally saved my life since imutils' tools weren't working the way i expected
    # https://towardsdatascience.com/optical-character-recognition-ocr-with-less-than-12-lines-of-code-using-python-48404218cccb
    
    return img

def getImages(url):
    full_img = imutils.url_to_image(url)

    vaccinated = processImg(full_img[253:310, 100:623]) # amount of applied vaccinations
    # second_doses = processImg(full_img[344:386, 102:619]) # amount of second doses applied
    second_doses = processImg(full_img[319:367, 100:612]) # uncomment if the other one presents problems

    # full_img[y1:y2, x1:x2] crops the image to a specific position

    # cv2.imwrite("vaccinated.jpeg", vaccinated)
    # cv2.imwrite("second_doses.jpeg", second_doses)
    ## Only uncomment these to troubleshoot!

    return getImgText(vaccinated), getImgText(second_doses)

def main():

    data = pd.Series({
        "location": "Colombia",
        "date": str(datetime.datetime.now(pytz.timezone("America/Bogota")).date() - datetime.timedelta(days=1)),
        "source_url": "https://www.minsalud.gov.co/salud/publica/Vacunacion/Paginas/Vacunacion-covid-19.aspx",
        "vaccine": "Pfizer/BioNTech, Sinovac",
    })

    # Obtains both Image URL and text in Image.
    hostname, path = "https://www.minsalud.gov.co", "/salud/publica/Vacunacion/Paginas/Vacunacion-covid-19.aspx"
    imgURL = getImageURL(hostname, path)

    # Process Image via OpenCV and tesseract.
    imgText = getImages(imgURL)
    total_vaccinations = imgText[0]
    people_fully_vaccinated = imgText[1]
    # print(imgText)

    # Eliminates all the involved strings if there's any
    total_vaccinations = int(re.sub(r"[^\d]", "", total_vaccinations))
    people_fully_vaccinated = int(re.sub(r"[^\d]", "", people_fully_vaccinated))
    # print("Total of vaccinations: %i" % (total_vaccinations))
    # print("Fully vaccinated: %i" % (people_fully_vaccinated))
    
    data["total_vaccinations"] = total_vaccinations # the government counts both first AND second doses in total vaccinations
    data["people_vaccinated"] = total_vaccinations - people_fully_vaccinated
    data["people_fully_vaccinated"] = people_fully_vaccinated

    vaxutils.increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == '__main__':
    main()
