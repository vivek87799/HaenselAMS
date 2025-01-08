import requests
import logging
import json
import asyncio
import aiohttp

import pandas as pd

# --- Constants ---
## API Key to be added as a AIRFLOW variable in production code
api_key = '18e7fdc7c-2aee-4cd7-b4e7-cec563ded602'
CONV_TYPE_ID = "data_engineering_challenge"
API_URL = f"https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={CONV_TYPE_ID}".format(CONV_TYPE_ID = CONV_TYPE_ID)


# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

async def send_batch_async(session, customer_journeys):
    """
    Sends a batch of data to the API endpoint asynchronously.

    Args:
      session: An aiohttp.ClientSession object.
      customer_journeys: A list of dictionaries representing the data to send.
    Return:
      A list of dicts 
    """
    try:
        body = {
            'customer_journeys': customer_journeys,
        }        
        
        
        async with session.post(
                API_URL, 
                data=json.dumps(body), 
                headers= {
                    'Content-Type': 'application/json',    
                    'x-api-key': api_key
                }
            ) as response:
            results = await response.json()
            logging.info(f"Status Code: {results['statusCode']}")
            logging.info(f"Partial Failure Errors: {results['partialFailureErrors']}")
            return results
           
    except Exception as e:
        logging.error(f"Error sending batch: {e}")


def concat_batch_data(file_path):
  """
  Reads data from a JSON file, processes it, and returns a Pandas DataFrame.

  Args:
    file_path: The path to the JSON file.

  Returns:
    A Pandas DataFrame containing the processed data.
  """
  try:
    df_list = []  # List to store DataFrames

    with open(file_path, 'r') as f:
        data = json.load(f)

    for item in data:
        if item['statusCode'] in (200, 206):
            df_list.append(pd.DataFrame(item['value']))

    # Concatenate all DataFrames in the list
    df = pd.concat(df_list, ignore_index=True)

    return df

  except Exception as e:
     logging.error(f"Error in concat_batch_data function: {e}")


def rescale_group(df):
  """Normalizes the columns 'initializer', 'holder', 'closer', and 'ihc' within each group.

  Args:
    df: A pandas DataFrame with columns 'initializer', 'holder', 'closer', and 'ihc'.

  Returns:
    A pandas DataFrame with normalized values for the specified columns.
  """
  cols_to_normalize = ['initializer', 'holder', 'closer', 'ihc']
  for col in cols_to_normalize:
    df[col] = df[col] / df[col].sum()
  return df