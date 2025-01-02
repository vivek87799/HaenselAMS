"""
 Each step should run as separate task in Apache Airflow. 
    - Query Data from a database 
    - Transform data as is necessary
    - Send transformed data to an API which returns you attribution results
    - Write attribution results to the database
    - Query and export data from the database

"""
from datetime import datetime, timedelta


# Query data from the database
def query_data():
    """
    Queries data from the database and saves it as a JSON file.
    Returns the file path.
    """
    pass

def transform_data(file_path: str)->str:
    """
    Reads data from the JSON file on which the transformation is done and again persisted as a JSON file
    Return the file path of the transformed data
    """

def api_call(file_path: str)-> str:
    """
    Sends the transformed data to the API and saves the API response as a JSON file.
    Returns the path to the API result
    """

def calc_attribution(file_path: str)-> None:
    """
    Calculates the overall IHC attribution 
    Writes the result to the database
    """

def export_data()->None:
    """
    Query the data from the database and export it as a csv file.
    """