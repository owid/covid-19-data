import datetime
import os
import re
import PyPDF2
import vaxutils


def main():

    url = "https://www.gov.bm/sites/default/files/COVID-19%20Vaccination%20Updates.pdf"
    os.system(f"curl {url} -o bermuda.pdf -s")

    with open("bermuda.pdf", "rb") as pdfFileObj:
        pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
        text = pdfReader.getPage(0).extractText()

    regex = r"VACCINATION CENTRE(.*?)Total Vaccines Administered"
    total_vaccinations = re.search(regex, text)
    total_vaccinations = vaxutils.clean_count(total_vaccinations.group(1))

    regex = r"As of (\w+ \d+, 20\d+)"
    date = re.search(regex, text)
    date = vaxutils.clean_date(date.group(1), "%B %d, %Y")

    vaxutils.increment(
        location="Bermuda",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url=url,
        vaccine="Pfizer/BioNTech"
    )

    os.remove("bermuda.pdf")


if __name__ == "__main__":
    main()
