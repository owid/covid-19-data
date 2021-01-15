import datetime
import json
import pytz
import requests
import pandas as pd
import vaxutils


def main():

    url = "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/Covid19_Vaccine_Administration_Data/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token="
    data = json.loads(requests.get(url).content)

    count = data["features"][0]["attributes"]["total_number_of_1st_dose_admini"]

    date = data["features"][0]["attributes"]["data_relevent_up_to_date"]
    date = str(pd.to_datetime(date, unit="ms").date())

    vaxutils.increment(
        location="Ireland",
        total_vaccinations=count,
        date=date,
        source_url="https://covid19ireland-geohive.hub.arcgis.com/",
        vaccine="Pfizer/BioNTech"
    )


if __name__ == "__main__":
    main()
