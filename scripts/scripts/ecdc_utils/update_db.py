#!/usr/bin/env python

import sys
import os
import pandas as pd
import pytz
import json
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

CURRENT_DIR = os.path.dirname(__file__)

sys.path.append(CURRENT_DIR)
from db import connection
from db_utils import DBUtils

sys.path.append(os.path.join(CURRENT_DIR, ".."))
import ecdc

# ID of user who imports the data
USER_ID = 29

# Dataset namespace
NAMESPACE = 'owid'

GRAPHER_DATASET_PATH = os.path.join(ecdc.OUTPUT_PATH, ecdc.DATASET_NAME + ".csv")
DEPLOY_QUEUE_PATH = os.getenv('DEPLOY_QUEUE_PATH')


def print_err(*args, **kwargs):
    return print(*args, file=sys.stderr, **kwargs)

def chunk_df(df, n):
    """Yield successive n-sized chunks from data frame."""
    for i in range(0, df.shape[0], n):
        yield df[i:i + n]


tz_utc = tz_db = timezone.utc
tz_local = datetime.now(tz_utc).astimezone().tzinfo
tz_london = pytz.timezone('Europe/London')


if __name__ == "__main__":
    with connection as c:
        db = DBUtils(c)

        # Check whether the database is up to date, by checking the
        # - last modified date of the Grapher file
        # - last modified date of the database row
        #
        # This is not bulletproof, but it allows for flexibility – authors could manually update
        # the repo, and that would trigger a database update too.

        (db_dataset_id, db_dataset_modified_time) = db.fetch_one("""
            SELECT id, dataEditedAt
            FROM datasets
            WHERE name = %s
            AND namespace = %s
        """, [ecdc.DATASET_NAME, NAMESPACE])

        db_dataset_modified_time = db_dataset_modified_time.replace(tzinfo=tz_db)
        file_modified_time = datetime.fromtimestamp(os.stat(GRAPHER_DATASET_PATH).st_mtime).replace(tzinfo=tz_local)

        if file_modified_time <= db_dataset_modified_time:
            print("Database is up to date")
            sys.exit(0)

        print("Updating database...")

        # Load dataset data frame

        df = pd.read_csv(GRAPHER_DATASET_PATH)

        # Check whether all entities exist in the database.
        # If some are missing, report & quit.

        entity_names = list(df['Country'].unique())

        db_entities_query = db.fetch_many("""
            SELECT id, name
            FROM entities
            WHERE name IN %s
        """, [entity_names])

        db_entity_id_by_name = { name: id for id, name in db_entities_query }

        # Terminate if some entities are missing from the database
        missing_entity_names = set(entity_names) - set(db_entity_id_by_name.keys())
        if len(missing_entity_names) > 0:
            print_err(f"Entity names missing from database: {str(missing_entity_names)}")
            sys.exit(1)

        # Fetch the source

        (db_source_id,) = db.fetch_one("""
            SELECT id
            FROM sources
            WHERE datasetId = %s
        """, db_dataset_id)

        # Check whether all variables match database variables.

        id_names = ["Country", "Year"]
        variable_names = list(set(df.columns) - set(id_names))

        db_variables_query = db.fetch_many("""
            SELECT id, name
            FROM variables
            WHERE datasetId = %s
        """, [db_dataset_id])

        db_variable_id_by_name = { name: id for id, name in db_variables_query }

        # Remove any variables no longer in the dataset. This is safe because any variables used in
        # charts won't be deleted because of database constrant checks.

        variable_names_to_remove = list(set(db_variable_id_by_name.keys()) - set(variable_names))
        if len(variable_names_to_remove):
            print(f"Removing variables: {str(variable_names_to_remove)}")
            variable_ids_to_remove = [ db_variable_id_by_name[n] for n in variable_names_to_remove ]
            db.execute("""
                DELETE FROM data_values
                WHERE variableId IN %(ids)s;
                DELETE FROM variables
                WHERE id IN %(ids)s;
            """, { 'ids': variable_ids_to_remove })

        # Add variables that didn't exist before. Make sure to set yearIsDay.

        variable_names_to_insert = list(set(variable_names) - set(db_variable_id_by_name.keys()))
        if len(variable_names_to_insert):
            print(f"Inserting variables: {str(variable_names_to_insert)}")
            for name in variable_names_to_insert:
                db_variable_id_by_name[name] = db.upsert_variable(
                    name=name,
                    code=None,
                    unit='',
                    short_unit=None,
                    source_id=db_source_id,
                    dataset_id=db_dataset_id,
                    display={
                        'yearIsDay': True,
                        'zeroDay': ecdc.ZERO_DAY
                    })

        # Delete all data_values in dataset

        print("Deleting all data_values...")

        db.execute("""
            DELETE FROM data_values
            WHERE variableId IN %s
        """, [tuple(db_variable_id_by_name.values())])

        # Insert new data_values

        print("Inserting new data_values...")

        df_data_values = df.melt(
            id_vars=id_names,
            value_vars=variable_names,
            var_name='variable',
            value_name='value'
        ).dropna(how='any')

        for df_chunk in chunk_df(df_data_values, 50000):
            data_values = [
                (row['value'], int(row['Year']), db_entity_id_by_name[row['Country']], db_variable_id_by_name[row['variable']])
                for _, row in df_chunk.iterrows()
            ]
            db.upsert_many("""
                INSERT INTO
                    data_values (value, year, entityId, variableId)
                VALUES
                    (%s, %s, %s, %s)
            """, data_values)

        # Update dataset dataUpdatedAt time & dataUpdatedBy

        db.execute("""
            UPDATE datasets
            SET
                dataEditedAt = NOW(),
                dataEditedByUserId = %s
            WHERE id = %s
        """, [USER_ID, db_dataset_id])

        # Update source name ("last updated at")

        time_str = datetime.now().astimezone(tz_london).strftime("%-d %B, %H:%M")
        source_name = f"European CDC – Situation Update Worldwide – Last updated {time_str} (London time)"

        db.execute("""
            UPDATE sources
            SET name = %s
            WHERE id = %s
        """, [source_name, db_source_id])

        # Update chart versions to trigger rebake

        db.execute("""
            UPDATE charts
            SET config = JSON_SET(config, "$.version", config->"$.version" + 1)
            WHERE id IN (
                SELECT DISTINCT chart_dimensions.chartId
                FROM chart_dimensions
                JOIN variables ON variables.id = chart_dimensions.variableId
                WHERE variables.datasetId = 5071
            )
        """)

        # Enqueue deploy

        if DEPLOY_QUEUE_PATH:
            with open(DEPLOY_QUEUE_PATH, 'a') as f:
                f.write(json.dumps({
                    'message': "Automatic ECDC update"
                }) + "\n")

    print("Database update successful.")
