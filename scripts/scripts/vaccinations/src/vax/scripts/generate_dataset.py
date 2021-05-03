import os

import pandas as pd


VAX_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
# Inputs
SUB_POP_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "../../input/owid/subnational_population_2020.csv"))
CONTINENTS_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "../../input/owid/continents.csv"))
EU_COUNTRIES_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "../../input/owid/eu_countries.csv"))
METADATA_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "./metadata.preliminary.csv"))
VAX_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "./vaccinations.preliminary.csv"))
# Outputs
AUTOMATED_STATE_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "./automation_state.csv"))


# Load files
subnational_pop = pd.read_csv(SUB_POP_FILE, usecols=["location", "population"])
continents = pd.read_csv(CONTINENTS_FILE, usecols=["Entity", "Unnamed: 3"])
eu_countries = pd.read_csv(EU_COUNTRIES_FILE, usecols=["Country"], squeeze=True).tolist()


# Aggregates
AGGREGATES = {
    "World": {
        "excluded_locs": ["England", "Northern Ireland", "Scotland", "Wales"], 
        "included_locs": None
    },
    "European Union": {
        "excluded_locs": None, 
        "included_locs": eu_countries
    }
}

for continent in ["Asia", "Africa", "Europe", "North America", "Oceania", "South America"]:
    AGGREGATES[continent] = {
        "excluded_locs": None,
        "included_locs": continents.loc[continents["Unnamed: 3"] == continent, "Entity"].tolist()
    }


def generate_automation_file(df: pd.DataFrame):
    df.sort_values(by=["automated", "location"]).to_csv(
        AUTOMATED_STATE_FILE, 
        index=False,
        columns=["automated", "location"],
    )


def generate_locations_file(df_metadata: pd.DataFrame, df_vax: pd.DataFrame):
    df_vax = df_vax.sort_values(by="date")
    raise NotImplementedError()
    """
    vax_per_loc <- vax[, .(vaccines = paste0(sort(unique(unlist(str_split(vaccine, ", ")))), collapse = ", ")), location]
    latest_meta <- vax[, .SD[.N], location]
    metadata <- merge(merge(metadata, vax_per_loc, "location"), latest_meta, "location")
    setnames(metadata, c("source_url", "date"), c("source_website", "last_observation_date"))
    metadata <- add_iso(metadata)
    metadata <- metadata[, c("location", "iso_code", "vaccines", "last_observation_date", "source_name", "source_website")]
    fwrite(metadata, "../../../public/data/vaccinations/locations.csv")
    return(metadata)
    """


def main():
    # Load data
    metadata = pd.read_csv(METADATA_FILE)
    vax = pd.read_csv(VAX_FILE, parse_dates=["date"])

    # Metadata
    generate_automation_file(metadata)
    metadata = generate_locations_file(metadata, vax)


if __name__ == "__main__":
    main()
