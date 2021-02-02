import datetime
import os
import re
import PyPDF2
import vaxutils


def main():

    date = datetime.date.today() - datetime.timedelta(days=1)
    url_date = date.strftime("%-d.%-m.%y")
    url = f"http://www.covidmaroc.ma/Documents/BULLETIN/{url_date}.COVID-19.pdf"
    
    os.system("curl http://www.covidmaroc.ma/Documents/BULLETIN/1.2.21.COVID-19.pdf -o morocco.pdf -s")

    with open("morocco.pdf", "rb") as pdfFileObj:
        pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
        text = pdfReader.getPage(0).extractText()

    regex = r"Bénéficiaires de la vaccination\s+Cumul global([\d\s]+)Situation épidémiologique"

    total_vaccinations = re.search(regex, text)
    total_vaccinations = vaxutils.clean_count(total_vaccinations.group(1))

    date = str(date)

    vaxutils.increment(
        location="Morocco",
        total_vaccinations=total_vaccinations,
        date=date,
        source_url=url,
        vaccine="Oxford/AstraZeneca, Sinopharm"
    )

    os.remove("morocco.pdf")


if __name__ == "__main__":
    main()
