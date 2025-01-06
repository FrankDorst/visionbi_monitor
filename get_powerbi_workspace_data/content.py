from datetime import datetime
import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from azure.keyvault.secrets import SecretClient
    from modules.custom_logger import LoggingManager
    from modules.datalake_writer import DataLakeWriter

async def process_powerbi_content(client: str, secret_client: "SecretClient", 
                                datalake_writer: "DataLakeWriter", logger: "LoggingManager") -> None:
    """Process PowerBI workspace content for a client"""
    from modules.powerbi_api import PowerBIRestAPI
    
    api = PowerBIRestAPI(client, secret_client, logger)
    workspaces = api.get_workspaces()
    workspace_ids = [x['id'] for x in workspaces]
    scans = api.post_workspace_scan(workspace_ids)
    
    # Wait for scan completion
    await asyncio.sleep(90)
    
    scan_result = api.get_workspace_scans(scans)
    workspace_content = scan_result['workspaces']
    datasources = scan_result['datasourceInstances']

    current_date = datetime.now().strftime("%d%m%Y")
    datalake_writer.write_json_data(workspace_content, "test-app", f"{client}_workspace_content_{current_date}")
    datalake_writer.write_json_data(datasources, 'test-app', f"{client}_datasources_{current_date}"
    logger.write_log(client, 'powerbi', 'INFO', f'Processed content for {len(workspaces)} workspaces') 