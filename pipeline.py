#Import necessary libraries and frameworks
import requests
import json
from google.cloud import bigquery
import pandas as pd
import os

#Set API key and endpoint
api_key = "APIKEY"
api_endpoint = "https://api.etherscan.io/api"

#Set list of smart contract addresses and block ranges
addresses = ['0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984', '0x853d955aCEf822Db058eb8505911ED77F175b99e']
from_blocks = [16000000, 16000001]
to_blocks = [16000200, 16000201]

#Define function to extract logs from Etherscan
def extract_logs(address, from_block, to_block):
  # Set parameters for logs API call
  logs_params = {'module': 'logs',
                 'action': 'getLogs',
                 'address': address,
                 'fromBlock': from_block,
                 'toBlock': to_block,
                 'apikey': api_key,
                }  # Note: added these parameters
  # Set parameters for blocks API call
  blocks_params = {'module': 'block',
                   'action': 'getblockreward',
                   'blockno': from_block,
                   'toBlockno': to_block,
                   'apikey': api_key}
  
  # Return logs_params and blocks_params
  return logs_params, blocks_params


# Call the extract_logs() function
extracted_logs = extract_logs(addresses[0], from_blocks[0], to_blocks[0])
#print(logs_params)

# Extract logs and blocks from Etherscan
logs_response = requests.get(api_endpoint, params=extracted_logs[0])
logs = logs_response.json()['result']

#print(logs_response.json())


# Transform logs into a format compatible with BigQuery table
transformed_logs = []
for log in logs:
    transformed_log = {
        'block_number': log.get('blockNumber', None),
        'transaction_hash': log.get('transactionHash', None),
        'from_address': log.get('address', None),
        'to_address': log.get('toAddress', None),
        'value': log.get('value', None),
        'timestamp': log.get('timeStamp', None),
        'transaction_type': log.get('type', None),
        'gas': log.get('gas', None),
        'gas_price': log.get('gasPrice', None),
        'nonce': log.get('nonce', None),
        'input': log.get('input', None),
        'contract_address': log.get('contractAddress', None),
        'cumulative_gas_used': log.get('cumulativeGasUsed', None),
        'gas_used': log.get('gasUsed', None),
        'confirmations': log.get('confirmations', None),
        'block_hash': log.get('blockHash', None),
        'block_parent_hash': log.get('blockParentHash', None),
        'block_sha3_uncles': log.get('blockSha3Uncles', None),
        'block_miner': log.get('blockMiner', None),
        'block_difficulty': log.get('blockDifficulty', None),
        'block_total_difficulty': log.get('blockTotalDifficulty', None),
        'block_size': log.get('blockSize', None),
        'block_gas_limit': log.get('blockGasLimit', None),
        'block_gas_used': log.get('blockGasUsed', None),
        'block_timestamp': log.get('blockTimestamp', None),
        'block_extra_data': log.get('blockExtraData', None),
        'log_index': log.get('logIndex', None),
        'log_transaction_index': log.get('logTransactionIndex', None),
        'log_type': log.get('log_type', None),  # Note: added this column
        'log_data': log.get('log_data', None),  # Note: added this column
        'log_topics': log.get('log_topics', None),  # Note: added this column
        'log_removed': log.get('log_removed', None),  # Note: added this column
    }
    transformed_logs.append(transformed_log)


# Set the path to your Google Cloud credentials file
GOOGLE_APPLICATION_CREDENTIALS = "/Users/macf/Downloads/mvp1-371016-147069e9e640.json"

# Set the Google Cloud project ID
GOOGLE_CLOUD_PROJECT = "mvp1-371016"

# Set environment variable for authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS
os.environ["GOOGLE_CLOUD_PROJECT"] = GOOGLE_CLOUD_PROJECT


# Connect to BigQuery
client = bigquery.Client()
#dataset_id = "mvp1-371016"  
#logs_table_id = "ether_mvp1.logs" 
#logs_table = client.get_table(f"{dataset_id}.{logs_table_id}")


# Create a DataFrame object from the transformed_logs data
df = pd.DataFrame(transformed_logs)

# Load the DataFrame into the BigQuery table
df.to_gbq(f"{dataset_id}.{logs_table_id}", if_exists="replace")

# Check if the data was loaded successfully
print("Data loaded successfully!")
