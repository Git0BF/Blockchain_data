# Import necessary libraries and frameworks
from google.cloud import bigquery
import os

# Set the path to your Google Cloud credentials file
GOOGLE_APPLICATION_CREDENTIALS = "/Users/macf/Downloads/mvp1-371016-147069e9e640.json"

# Set the Google Cloud project ID
GOOGLE_CLOUD_PROJECT = "mvp1-371016"

# Set environment variable for authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS
os.environ["GOOGLE_CLOUD_PROJECT"] = GOOGLE_CLOUD_PROJECT

# Connect to BigQuery
client = bigquery.Client()

# Set up BigQuery dataset and table
dataset_id = "ether_mvp1"

# Delete the existing dataset
client.delete_dataset(dataset_id, delete_contents=True)

# Create a new dataset in BigQuery
dataset = client.create_dataset(dataset_id)

# Define the schema of the logs table
logs_schema = [
    bigquery.SchemaField("block_number", "INTEGER"),
    bigquery.SchemaField("transaction_hash", "STRING"),
    bigquery.SchemaField("from_address", "STRING"),
    bigquery.SchemaField("to_address", "STRING"),
    bigquery.SchemaField("value", "FLOAT"),
    bigquery.SchemaField("timestamp", "INTEGER"),
    bigquery.SchemaField("transaction_type", "STRING"),
    bigquery.SchemaField("gas", "INTEGER"),
    bigquery.SchemaField("gas_price", "INTEGER"),
    bigquery.SchemaField("nonce", "INTEGER"),
    bigquery.SchemaField("input", "STRING"),
    bigquery.SchemaField("contract_address", "STRING"),
    bigquery.SchemaField("cumulative_gas_used", "INTEGER"),
    bigquery.SchemaField("gas_used", "INTEGER"),
    bigquery.SchemaField("confirmations", "INTEGER"),
    bigquery.SchemaField("block_hash", "STRING"),
    bigquery.SchemaField("block_parent_hash", "STRING"),
    bigquery.SchemaField("block_sha3_uncles", "STRING"),
    bigquery.SchemaField("block_miner", "STRING"),
    bigquery.SchemaField("block_difficulty", "FLOAT"),
    bigquery.SchemaField("block_total_difficulty", "FLOAT"),
    bigquery.SchemaField("block_size", "INTEGER"),
    bigquery.SchemaField("block_gas_limit", "INTEGER"),
    bigquery.SchemaField("block_gas_used", "INTEGER"),
    bigquery.SchemaField("block_timestamp", "INTEGER"),
    bigquery.SchemaField("block_extra_data", "STRING"),
    bigquery.SchemaField("log_index", "INTEGER"),
    bigquery.SchemaField("log_transaction_index", "INTEGER"),
    bigquery.SchemaField("log_type", "STRING"),
    bigquery.SchemaField("log_data", "STRING"),
    bigquery.SchemaField("log_topics", "STRING"),
    bigquery.SchemaField("log_removed", "BOOLEAN")
]

# Create the logs table in BigQuery
logs_table_id = f"{GOOGLE_CLOUD_PROJECT}.{dataset_id}.logs"
logs_table = bigquery.Table(logs_table_id, schema=logs_schema)
logs_table = client.create_table(logs_table)

# Print confirmation message
print("Successfully created BigQuery table for logs")
