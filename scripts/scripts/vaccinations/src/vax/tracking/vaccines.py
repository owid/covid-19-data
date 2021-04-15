import os

import pandas as pd
import requests
from bs4 import BeautifulSoup

from vax.utils.utils import get_soup, get_headers


CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))

VAX_MAPPING = {
    "Bharat Biotech": "Covaxin",
    "FBRI": "EpiVacCorona",
    "Janssen (Johnson & Johnson)": "Johnson&Johnson",
    "Moderna": "Moderna",
    "Oxford/AstraZeneca": "Oxford/AstraZeneca",
    "Pfizer/BioNTech": "Pfizer/BioNTech",
    "Sinopharm (Beijing)": "Sinopharm/Beijing",
    "Sinopharm (Wuhan)": "Sinopharm/Wuhan",
    "Sinovac": "Sinovac",
    "Gamaleya": "Sputnik V",
    "Serum Institute of India": "Oxford/AstraZeneca"
}


COUNTRY_MAP = {
    "United Kingdom": "united-kingdom-of-great-britain-and-northern-ireland",
    "United States": "united-states-of-america",
    "Venezuela": "venezuela-bolivarian-republic-of",
    "South Korea": "republic-of-korea",
    "Scotland": "united-kingdom-of-great-britain-and-northern-ireland",
    "Wales": "united-kingdom-of-great-britain-and-northern-ireland",
    "England": "united-kingdom-of-great-britain-and-northern-ireland",
    "Northern Ireland": "united-kingdom-of-great-britain-and-northern-ireland",
    "Faeroe Islands": "faroe-islands/",
    "Cape Verde": "cabo-verde",
    "Brunei": "brunei-darussalam",
    "Bahamas": "the-bahamas",

}

class TrackVaccinesClient:
    """Client to interact with https://covid19.trackvaccines.org."""

    def __init__(self):
        self.base_url = "https://covid19.trackvaccines.org"

    def get_country_url(self, location: str):
        # Build URL
        if location in COUNTRY_MAP:
            location = COUNTRY_MAP[location]
        else:
            location = location.lower().replace(" ", "-")
        url = f"{self.base_url}/country/{location}/"
        if not self._valid_url(url):
            raise ValueError(f"Couldn't find vaccines for {location}. Check {url}")
        return url

    @property
    def all_vaccines_url(self):
        return f"{self.base_url}/vaccines/"

    def _valid_url(self, url: str):
        resp = requests.get(url, headers=get_headers())
        if not resp.ok:
            return False
        return True

    def vaccines_approved(self, location: str = None, original_names: bool = False) -> list:
        """Get list of approved vaccines in a country (or all if None specified).

        Args:
            location (str, optional): Country name. If None, retrieves all approved vaccines. Defaults to None.
            original_names (bool, optional): Set to True to keep vaccine from web. Defaults to False.

        Returns:
            list: Approved vaccines
        """
        if location:
            try:
                url = self.get_country_url(location)
                soup = get_soup(url)
                return self._parse_vaccines_location(soup, original_names)
            except ValueError:
                return None
        else:
            soup = get_soup(self.all_vaccines_url)
            return self._parse_vaccines_all(soup, original_names)

    def _parse_vaccines_location(self, soup: BeautifulSoup, original_names: bool = False):
        content = soup.find(class_="card-grid alignwide")
        vaccines_html = content.find_all(class_="card__title has-text-align-center")
        vaccines = [e.find("span").text for e in vaccines_html]
        if original_names:
            return vaccines
        return list(set(map(map_vaccine, vaccines)))

    def _parse_vaccines_all(self, soup: BeautifulSoup, original_names: bool = False):
        vaccines_html = soup.find("ul", class_="card-grid alignwide").find_all("h2")
        vaccines = [vax.find("span").text for vax in vaccines_html]
        if original_names:
            return vaccines
        return list(set(map(map_vaccine, vaccines)))


def map_vaccine(vaccine):
    if vaccine in VAX_MAPPING:
        return VAX_MAPPING[vaccine]
    return vaccine


def vaccines_tracked(path_locations: str = None, location: str = None, as_list: bool = False) -> pd.DataFrame:
    """Get tracked vaccines for tracked countries.

    Args:
        path_locations (str, optional): Path to locations csv file. 
                                        Default value works if repo structure is left unmodified.
        location (str, optional): Country name. Defaults to None.
        as_list (bool, optional): Set to True to return a (flattened) list.

    Returns:
        pd.DataFrame: Dataframe with location and vaccines tracked.
    """
    if not path_locations:
        path_locations = (
            os.path.abspath(os.path.join(CURRENT_DIR, "../../../../../../public/data/vaccinations/locations.csv"))
        )
    df = pd.read_csv(path_locations, usecols=["vaccines", "location"])
    df = df.assign(vaccines=df.vaccines.apply(lambda x: set(x.split(', '))))
    if location:
        if isinstance(location, str):
            location = [location]
        df = df[df.location.isin(location)]
    if as_list:
        return list(set([vv for v in df.vaccines for vv in v]))
    return df

def vaccines_approved(path_locations: str = None) -> pd.DataFrame:
    """Get approved vaccines for tracked countries.

    This may take between 2-3 minutes.

    Args:
        path_locations (str, optional): Path to locations csv file. 
                                        Default value works if repo structure is left unmodified.

    Returns:
        pd.DataFrame: Dataframe with location and vaccines approved.
    """
    if not path_locations:
        path_locations = (
            os.path.abspath(os.path.join(CURRENT_DIR, "../../../../../../public/data/vaccinations/locations.csv"))
        )
    df = pd.read_csv(path_locations, usecols=["location"])
    client = TrackVaccinesClient()
    y = df.location.apply(lambda x: client.vaccines_approved(x))
    return df.assign(vaccines=y.apply(lambda x: set(x) if x is not None else None))


def vaccines_missing(aggregated: bool = False):
    """Get missing vaccines.

    - Columns "_unapproved" mean vaccines not approved but currently being administered.
    - Columns "_untracked" mean vaccines approved but not tracked.

    Note: Unapproved might mean that trackvaccines.org are not counting a vaccine that was actually approved.

    Args:
        aggregated (bool, optional): Set to True to get list of untracked/unapproved global vaccines. Defaults to False.

    Returns:
        Union[pd.DataFrame, dict]: Unapproved/untracked vaccines
    """
    if aggregated:
        # Get tracked vaccines
        vax_tracked = vaccines_tracked(as_list=True)
        client = TrackVaccinesClient()
        vax_approved = client.vaccines_approved()
        return {
            "vaccines_untracked": [v for v in vax_approved if v not in vax_tracked],
            "vaccines_unapproved": [v for v in vax_tracked if v not in vax_approved]
        }
    else:
        vax_tracked = vaccines_tracked()
        vax_approved = vaccines_approved()
        # Build result dataframe
        df = vax_tracked.merge(vax_approved, on="location", suffixes=("_tracked", "_approved"))
        df = df[df.vaccines_tracked != df.vaccines_approved].dropna()
        df = df.assign(
            unapproved=(
                df.apply(lambda x: [xx for xx in x["vaccines_tracked"] if xx not in x["vaccines_approved"]], axis=1)
            ),
            untracked=(
                df.apply(lambda x: [xx for xx in x["vaccines_approved"] if xx not in x["vaccines_tracked"]], axis=1)
            )
        )
        df = df.assign(
            num_unapproved=df.unapproved.apply(len),
            num_untracked=df.untracked.apply(len)
        )
        df = df[["location", "unapproved", "num_unapproved", "untracked", "num_untracked"]]
        df = df.sort_values(by="num_untracked", ascending=False)
        return df
