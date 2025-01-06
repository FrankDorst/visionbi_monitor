from datetime import datetime
import asyncio
from typing import TYPE_CHECKING, List
from functions.activity_dates import get_todo_dates

if TYPE_CHECKING:
    from azure.keyvault.secrets import SecretClient
    from modules.custom_logger import LoggingManager
    from modules.datalake_writer import DataLakeWriter
    from modules.powerbi_api import PowerBIRestAPI

async def process_powerbi_activity(api: "PowerBIRestAPI", date: datetime, 
                                 client: str, datalake_writer: "DataLakeWriter") -> None:
    """Process activities for a single date"""
    activities = api.get_tenant_activities(date)
    file_date = date.strftime("%d%m%Y")
    datalake_writer.write_json_data(activities, "test-app", f"{client}_activities_{file_date}")

async def process_powerbi_activities(client: str, secret_client: "SecretClient", 
                                   datalake_writer: "DataLakeWriter", logger: "LoggingManager") -> None:
    """Process PowerBI activities for a client""" 
    from modules.powerbi_api import PowerBIRestAPI
    
    api = PowerBIRestAPI(client, secret_client, logger)
    dates = get_todo_dates(client, datalake_writer)
    activity_tasks = [
        process_powerbi_activity(api, date, client, datalake_writer) 
        for date in dates
    ]
    await asyncio.gather(*activity_tasks)
    logger.write_log(client, 'powerbi', 'INFO', f'Processed activities for {len(dates)} dates') 