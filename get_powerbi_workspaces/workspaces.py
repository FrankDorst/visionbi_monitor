from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from azure.keyvault.secrets import SecretClient
    from modules.custom_logger import LoggingManager
    from modules.datalake_writer import DataLakeWriter

async def process_powerbi_workspaces(client: str, secret_client: "SecretClient", 
                                   datalake_writer: "DataLakeWriter", logger: "LoggingManager") -> None:
    """Process PowerBI workspaces for a client"""
    from modules.powerbi_api import PowerBIRestAPI
    
    api = PowerBIRestAPI(client, secret_client, logger)
    workspaces = api.get_workspaces()
    current_date = datetime.now().strftime("%d%m%Y")
    datalake_writer.write_json_data(workspaces, "test-app", f"{client}_workspaces_{current_date}")
    logger.write_log(client, 'powerbi', 'INFO', f'Processed {len(workspaces)} workspaces') 