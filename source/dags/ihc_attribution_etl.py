"""
 Each step should run as separate task in Apache Airflow. 
    - Query Data from a database 
    - Transform data as is necessary
    - Send transformed data to an API which returns you attribution results
    - Write attribution results to the database
    - Query and export data from the database

"""

import logging
import json
import gc
from datetime import datetime, timedelta
from itertools import chain

import aiohttp
import asyncio
import pandas as pd
import requests
import sqlite3
from airflow.decorators import dag, task
from airflow.models import Variable

from helper_functions import rescale_group, send_batch_async, concat_batch_data

# --- Constants ---
API_ENDPOINT = "your_api_endpoint"
DATA_FOLDER = "/opt/airflow/data/"
DB_PATH = DATA_FOLDER + "challenge.db"
SQL_FILE_PATH = DATA_FOLDER + "challenge_db_create.sql"
MAX_CONVERSION_IDS = 100
SESSION_LOWER_LIMIT_DATE = Variable.get("SESSION_LOWER_LIMIT_DATE", default_var=0)
MAX_CUSTOMER_JOURNEYS = Variable.get("MAX_CUSTOMER_JOURNEYS", default_var=100)
MAX_SESSIONS = Variable.get("MAX_SESSIONS", default_var=3000)


# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# --- Airflow DAG ---
default_args = {
    "owner": "Haensel_AMS",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="ihc_attribution_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 6),
    schedule_interval="@daily",
)
def ihc_attribution_etl():
    @task()
    def init_database():
        """Creates the tables for the database."""
        try:
            with sqlite3.connect(DB_PATH) as con:
                cursor = con.cursor()
                with open(SQL_FILE_PATH, "r") as sql_file:
                    sql_script = sql_file.read()
                cursor.executescript(sql_script)
                con.commit()
            logging.info(f"Successfully executed SQL script on {DB_PATH}")
        except Exception as e:
            logging.error(f"Error executing SQL script: {e}")

    @task()
    def query_data() -> str:
        """
        Queries data from the database, converts to DataFrame, saves as a JSON file.
        Returns:
            A file path to the queried and preprocessed data persisted as JSON file.
        """
        file_path = DATA_FOLDER + "output.json"
        try:
            with sqlite3.connect(DB_PATH) as conn:
                df_session_sources = pd.read_sql_query(
                    """SELECT * FROM session_sources""", conn
                )
                df_conversions = pd.read_sql_query(
                    """SELECT * FROM conversions""", conn
                )

            merged_df = pd.merge(
                df_session_sources, df_conversions, on="user_id", how="inner"
            )

            merged_df["event_datetime"] = pd.to_datetime(
                merged_df["event_date"] + " " + merged_df["event_time"]
            )
            merged_df["conv_datetime"] = pd.to_datetime(
                merged_df["conv_date"] + " " + merged_df["conv_time"]
            )
            merged_df = merged_df.drop(
                ["conv_date", "event_date", "conv_time", "event_time"], axis=1
            )

            merged_df.to_json(file_path, orient="records")
            logging.info(f"Data queried and saved to {file_path}")
            return file_path

        except sqlite3.Error as e:
            logging.error(f"Error querying data: {e}")

    @task()
    def transform_data(file_path: str) -> str:
        """
        Reads data from the JSON file, performs transformations,
        and saves the transformed data as a new JSON file.
        Args:
            file_path: file path to the JSON file that has queried and pre-processed data from the tables 
        Returns: 
            File path to the transformed data stored as JSON file.
        """
        try:
            df = pd.read_json(file_path, orient="records")
            df["event_datetime"] = pd.to_datetime(df["event_datetime"], unit="ms")
            df["conv_datetime"] = pd.to_datetime(df["conv_datetime"], unit="ms")

            # Set in Airflow variables
            session_lower_limit_date = 0

            if session_lower_limit_date:
                cutoff_date = datetime.now() - timedelta(days=session_lower_limit_date)
                df = df[(df["event_datetime"] >= cutoff_date) & (df["event_datetime"] <= df["conv_datetime"])] 
            else:
                df = df[(df["event_datetime"] <= df["conv_datetime"])]

            logging.debug(df.head())

            df["conversion"] = (
                (df["conv_datetime"] == df["event_datetime"])
            ).astype(int)

            df["timestamp"] = pd.to_datetime(
                df["event_datetime"], format="%Y-%m-%d %H:%M:%S"
            ).astype(str)

            df = df.drop(
                ["user_id", "revenue", "event_datetime", "conv_datetime"], axis=1
            )
            df = df.rename(
                columns={"conv_id": "conversion_id", "channel_name": "channel_label"}
            )

            transformed_file_path = DATA_FOLDER + "transformed_output.json"
            df.to_json(transformed_file_path, orient="records")
            logging.info(f"Transformed data saved to {transformed_file_path}")

            return transformed_file_path

        except Exception as e:
            logging.error(f"Error transforming data: {e}")
            raise

    @task()
    def api_call(file_path: str) -> str:
        """
        API call to get the attribution values. 

        Args:
            file_path: File path of the transformed data persisted as JSON file.

        Returns the path to the API result file.
        """
        
        results = []

        try:
            with open(file_path, "r") as json_file:
                session_data = json.load(json_file)

            logging.info("Loaded data from file:")
            logging.debug(session_data)  # Use debug for potentially large data

            async def process_batches():
                current_batch = []
                current_conversion_ids = set()
                current_session_ids = set()
                nonlocal results

                async with aiohttp.ClientSession() as session:
                    for event in session_data:
                        conversion_id = event["conversion_id"]
                        session_id = event["session_id"]

                        if (
                            len(current_conversion_ids) < MAX_CONVERSION_IDS
                            and len(current_session_ids) < MAX_SESSIONS
                        ):
                            current_batch.append(event)
                            current_conversion_ids.add(conversion_id)
                            current_session_ids.add(session_id)
                        else:
                            logging.info(
                                f"Starting API call with {len(current_conversion_ids)} Customer Journeys"
                                f"conversion IDs and {len(current_session_ids)} session IDs"
                            )
                            results.append(
                                await send_batch_async(session, current_batch)
                            )
                            current_batch = [event]  # Start a new batch
                            current_conversion_ids = {conversion_id}
                            current_session_ids = {session_id}

                    if current_batch:
                        results.append(await send_batch_async(session, current_batch))

            asyncio.run(process_batches())

            api_result_file_path = DATA_FOLDER + "api_result_data.json"
            with open(api_result_file_path, "w") as f:
                json.dump(results, f, indent=4)
            logging.info(f"API results saved to {api_result_file_path}")
            return api_result_file_path

        except Exception as e:
            logging.error(f"Error during API call: {e}")
        # --- Used to get the simulated api call data
        # return DATA_FOLDER + "api_results.json"
            
    
    @task()
    def write_attribution(file_path: str) -> None:
        """
        Calculates the overall IHC attribution and writes the result to the database.
        Args:
            file_path: File path to the results from the api call
        Returns: None
        """
        try:
            df = pd.read_json(file_path, orient="records")

            # Extract data from multiple batches
            df = concat_batch_data(file_path)
            df = df.rename(columns={"conversion_id": "conv_id"})
            df = df.drop(["initializer", "holder", "closer"], axis=1)

            # --- The below step will be required if a Customer journy could go beyond <MAX_SESSIONS> sessions
            # From the documentation we observed that thre returned ihc attribution is between 0-1
            ## df_rescale_group = df.groupby("conversion_id").apply(rescale_group)

            with sqlite3.connect(DB_PATH) as conn:
                # Write the DataFrame to the database
                df.to_sql("attribution_customer_journey", conn, if_exists="replace", index=False)
            
                # Read data from tables into Pandas DataFrames
                session_sources_df = pd.read_sql_query("SELECT * FROM session_sources", conn)
                session_costs_df = pd.read_sql_query("SELECT * FROM session_costs", conn)
                # Data from the table is read again for readability 
                attribution_customer_journey_df = pd.read_sql_query("SELECT * FROM attribution_customer_journey", conn)
                conversions_df = pd.read_sql_query("SELECT * FROM conversions", conn)

                # Perform the data transformations and aggregations
                # --- Calculate channel_reporting_cost ---
                session_costs_df['cost'] = session_costs_df['cost'].fillna(0)
                merged_data_cost = pd.merge(session_sources_df, session_costs_df, on='session_id')
                channel_reporting_cost = merged_data_cost.groupby(['channel_name', 'event_date'])['cost'].sum().reset_index()
                channel_reporting_cost = channel_reporting_cost.rename(columns={'event_date': 'date'})

                # --- Calculate channel_reporting_ihc ---
                merged_data_ihc = pd.merge(attribution_customer_journey_df, session_sources_df, on='session_id')
                channel_reporting_ihc = merged_data_ihc.groupby(['channel_name', 'event_date'])['ihc'].sum().reset_index()
                channel_reporting_ihc = channel_reporting_ihc.rename(columns={'event_date': 'date'})
                

                # --- Calculate channel_reporting_ihc_revenue ---
                merged_conversions = pd.merge(conversions_df, attribution_customer_journey_df, on='conv_id')
                merged_data_revenue = pd.merge(merged_conversions, session_sources_df, on='session_id')
                merged_data_revenue['ihc_revenue'] = merged_data_revenue['ihc'] * merged_data_revenue['revenue']
                channel_reporting_ihc_revenue = merged_data_revenue.groupby(['channel_name', 'event_date'])['ihc_revenue'].sum().reset_index()
                channel_reporting_ihc_revenue = channel_reporting_ihc_revenue.rename(columns={'event_date': 'date'})
                

                # --- Merge all KPIs into channel_reporting ---
                channel_reporting_df = pd.merge(channel_reporting_cost, channel_reporting_ihc, on=['channel_name', 'date'], how='outer')
                channel_reporting_df = pd.merge(channel_reporting_df, channel_reporting_ihc_revenue, on=['channel_name', 'date'], how='outer')

                # Round the columns to 3 decimal places
                channel_reporting_df['cost'] = channel_reporting_df['cost'].round(2)
                channel_reporting_df['ihc'] = channel_reporting_df['ihc'].round(3)
                channel_reporting_df['ihc_revenue'] = channel_reporting_df['ihc_revenue'].round(3)
                # FIll NaNs
                channel_reporting_df['ihc'] = channel_reporting_df['ihc'].fillna(0)
                channel_reporting_df['ihc_revenue'] = channel_reporting_df  ['ihc_revenue'].fillna(0)

                # Persist the data into the channel_reporting table
                channel_reporting_df.to_sql("channel_reporting", conn, if_exists="replace", index=False)
                logging.debug(f"data persisted to channel_reporting: {channel_reporting_df.head()}")

            logging.info("Channel reporting table created successfully!")

        except (pd.errors.EmptyDataError, sqlite3.Error) as e:
            logging.error(f"Error writing attribution data: {e}")
        except Exception as e:
            logging.error(f"Error in attribution data module: {e}")
        finally:
            del session_sources_df
            del session_costs_df
            del channel_reporting_cost
            del channel_reporting_ihc
            del channel_reporting_ihc_revenue
            del channel_reporting_df
            del attribution_customer_journey_df
            gc.collect() 

    @task()
    def export_data() -> None:
        """Queries data from the database, calculates two new KPIs CPO AND ROAS exports it along with other KPIs 
            as a CSV file.
        """
        try:
            with sqlite3.connect(DB_PATH) as conn:
                channel_reporting_df = pd.read_sql_query(
                    "SELECT * FROM channel_reporting", conn
                )
            
            logging.debug(f"data to be reported: {channel_reporting_df.head()}")
            # Calculate CPO and ROAS
            channel_reporting_df["CPO"] = (
                channel_reporting_df["cost"] / channel_reporting_df["ihc"]
            )
            channel_reporting_df["ROAS"] = (
                channel_reporting_df["ihc_revenue"] / channel_reporting_df["cost"]
            )

            # Replace infinite values with NaN (in case of division by zero)
            channel_reporting_df.replace(
                [float("inf"), float("-inf")], float("nan"), inplace=True
            )

            channel_reporting_df['CPO'] = channel_reporting_df['CPO'].round(3)
            channel_reporting_df['ROAS'] = channel_reporting_df['ROAS'].round(3)

            channel_reporting_df['CPO'] = channel_reporting_df['CPO'].fillna(0)
            channel_reporting_df['ROAS'] = channel_reporting_df  ['ROAS'].fillna(0)

            # Create a .csv file
            reporting_csv_file_path = DATA_FOLDER + "channel_reporting.csv"
            channel_reporting_df.to_csv(reporting_csv_file_path, index=False)

            logging.info(
                f"Channel reporting CSV file created successfully at {reporting_csv_file_path}"
            )

        except sqlite3.Error as e:
            logging.error(f"Error exporting data: {e}")


    # --- DAG Workflow ---
    init_db_task = init_database()
    query_data_task = query_data()
    transform_data_task = transform_data(query_data_task)
    api_call_task = api_call(transform_data_task) 
    write_attribution_task = write_attribution(api_call_task)
    export_data_task = export_data()

    (
        init_db_task
        >> query_data_task
        >> transform_data_task
        >> api_call_task
        >> write_attribution_task
        >> export_data_task
    )

greet_dag = ihc_attribution_etl()
