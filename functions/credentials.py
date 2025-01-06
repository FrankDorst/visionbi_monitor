import os 
from dotenv import load_dotenv

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from modules.datalake_writer import DataLakeWriter
from modules.custom_logger import LoggingManager
import os


def get_credentials() -> tuple[SecretClient, DataLakeWriter, LoggingManager]:
    """Initialize shared credentials and clients"""
    load_dotenv()

    # Get credentials from environment variables
    tenant_id = os.getenv('TENANT_ID')
    client_id = os.getenv('CLIENT_ID') 
    client_secret = os.getenv('CLIENT_SECRET')
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    
    # Initialize the SecretClient
    key_vault_url = os.getenv('KEYVAULT_URL')
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
    
    # Initialize DataLake writer
    connection_string = secret_client.get_secret("adls2-connection-string").value
    logger = LoggingManager()
    datalake_writer = DataLakeWriter(connection_string, logger)
    
    return secret_client, datalake_writer, logger


