import requests
import logging
import json
import asyncio
import aiohttp

import pandas as pd

## Insert API Key here
api_key = '8e7fdc7c-2aee-4cd7-b4e7-cec563ded602'

## Insert Conversion Type ID here
conv_type_id = 'data_engineering_challenge'

api_url = api_url = "https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={conv_type_id}".format(conv_type_id = conv_type_id)

async def send_batch_async(session, customer_journeys):
    """
    Sends a batch of data to the API endpoint asynchronously.

    Args:
      session: An aiohttp.ClientSession object.
      customer_journeys: A list of dictionaries representing the data to send.
    """
    try:
        body = {
            'customer_journeys': customer_journeys,
        }        
        
        
        async with session.post(
                api_url, 
                data=json.dumps(body), 
                headers= {
                    'Content-Type': 'application/json',    
                    'x-api-key': api_key
                }
            ) as response:
            results = await response.json()
            print(body)
            print(results)
            print(f"Status Code: {results['statusCode']}")
            print("-"*30)
            print(f"Partial Failure Errors: {results['partialFailureErrors']}")
            print("-"*30)
            return results
           
    except aiohttp.ClientError as e:
        print(f"Error sending batch: {e}")


def concat_batch_data(file_path):
  """
  Reads data from a JSON file, processes it, and returns a Pandas DataFrame.

  Args:
    file_path: The path to the JSON file.

  Returns:
    A Pandas DataFrame containing the processed data.
  """

  df_list = []  # List to store DataFrames

  with open(file_path, 'r') as f:
      data = json.load(f)

  for item in data:
      if item['statusCode'] in (200, 206):
          df_list.append(pd.DataFrame(item['value']))

  # Concatenate all DataFrames in the list
  df = pd.concat(df_list, ignore_index=True)

  return df

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