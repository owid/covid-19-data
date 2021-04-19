import os

import pandas as pd


CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))


def country_updates_summary(path_vaccinations: str = None, path_locations: str = None, 
                            path_automation_state: str = None, as_dict: bool = False, sortby_counts: bool = False):
    """Check last updated countries.

    It loads the content from locations.csv, vaccinations.csv and automation_state.csv to present results on the update
    frequency and timeline of all countries. By default, the countries are sorted from least to most recently updated.
    You can also sort them from least to most frequently updated ones by using argument `sortby_counts`.

    In Jupyter is recommended to ass the following lines to enable the DataFrame to be fully shown:

    ```python
    import pandas as pd
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_colwidth', None)
    ```

    Args:
        path_vaccinations (str, optional): Path to vaccinations csv file. 
                                            Default value works if repo structure is left unmodified.
        path_locations (str, optional): Path to locations csv file. 
                                        Default value works if repo structure is left unmodified.
        path_automation_state (str, optional): Path to automation state csv file.
                                                Default value works if repo structure is left unmodified.
        as_dict (bool, optional): Set to True for the return value to be shaped as a dictionary. Otherwise returns a 
                                    DataFrame.
        sortby_counts (bool, optional): Set to True to sort resuls from least to most updated countries.

    Returns:
        Union[pd.DataFrame, dict]: List or DataFrame, where each row (or element) contains five fields: 
                                    - 'last_observation_date': Last update date.
                                    - 'location': Country name.
                                    - 'source_website': Source used to retrieve last added data.
                                    - 'automated': True if country process is automated.
                                    - 'counts': Number of times the country has been updated.
    """
    # Get data paths
    if not path_vaccinations:
        path_vaccinations = (
            os.path.abspath(os.path.join(CURRENT_DIR, "../../../../../../public/data/vaccinations/vaccinations.csv"))
        )
    if not path_locations:
        path_locations = (
            os.path.abspath(os.path.join(CURRENT_DIR, "../../../../../../public/data/vaccinations/locations.csv"))
        )
    if not path_automation_state:
        path_automation_state = os.path.abspath(os.path.join(CURRENT_DIR, "../../../automation_state.csv"))
    # Read data
    df_vax = pd.read_csv(path_vaccinations)
    df_loc = pd.read_csv(path_locations)
    df_state = pd.read_csv(path_automation_state)
    # Get counts
    df_vax = pd.DataFrame({"counts": df_vax.groupby("location").date.count().sort_values()})
    # Merge data
    df = df_loc.merge(df_state, on="location")
    df = df.merge(df_vax, on="location")
    # Sort data
    if sortby_counts:
        sort_column = "counts"
    else:
        sort_column = "last_observation_date"
    df = df.sort_values(
        by=sort_column
    )[["location", "last_observation_date", "counts", "automated", "source_website"]]
    # Add columns
    def web_type(x):
        if ("facebook" in x.lower()) or ("twitter" in x.lower()):
            return "Social Network"
        elif "github" in x.lower():
            return "GitHub"
        elif ("gov" in x.lower()) or ("gob" in x.lower()):
            return "Govern"
        else:
            return "Others"
    df = df.assign(**{"web_type": df.source_website.apply(web_type)})
    # Return data
    if as_dict:
        return df.to_dict(orient="records")
    return df


def countries_missing(path_population: str = None, path_locations: str = None, ascending: bool = False,
                            as_dict: bool = False):
    """Get countries currently not present in our dataset.

    Args:
        path_population (str, optional): Path to UN population csv file. 
                                            Default value works if repo structure is left unmodified.
        path_locations (str, optional): Path to locations csv file. 
                                        Default value works if repo structure is left unmodified.
        ascending (bool, optional): Set to True to sort results in ascending order. By default sorts in ascedning order.
        as_dict (bool, optional): Set to True for the return value to be shaped as a dictionary. Otherwise returns a 
                                    DataFrame.
    """
    if not path_population:
        path_population = (
            os.path.abspath(os.path.join(CURRENT_DIR, "../../../../../input/un/population_2020.csv"))
        )
    if not path_locations:
        path_locations = (
            os.path.abspath(os.path.join(CURRENT_DIR, "../../../../../../public/data/vaccinations/locations.csv"))
        )
    df_loc = pd.read_csv(path_locations, usecols=["location"])
    df_pop = pd.read_csv(path_population)
    df_pop = df_pop[df_pop.iso_code.apply(lambda x: isinstance(x, str) and len(x)==3)]
    df_mis = df_pop.loc[~df_pop['entity'].isin(df_loc['location']), ["entity", "population"]]
    # Sort
    if not ascending:
        df_mis = df_mis.sort_values(by="population", ascending=False)
    # Return data
    if as_dict:
        return df_mis.to_dict(orient="records")
    return df_mis


