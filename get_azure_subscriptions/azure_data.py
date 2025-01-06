from datetime import datetime
import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from azure.keyvault.secrets import SecretClient
    from modules.custom_logger import LoggingManager
    from modules.datalake_writer import DataLakeWriter

async def process_azure_data(client: str, secret_client: "SecretClient", 
                           datalake_writer: "DataLakeWriter", logger: "LoggingManager") -> None:
    """Process Azure data for a client"""
    from modules.azure_api import AzureRestAPI
    
    api = AzureRestAPI(client, secret_client, logger)
    subscriptions = [
        x.name for x in secret_client.list_properties_of_secrets() 
        if client in x.name and "sub" in x.name
    ]
    

    async def get_azure_costs(subscription: str):
        id_ = secret_client.get_secret(subscription).value

        historical = [x for x in datalake_writer.list_files('test-app') if 'historical' in x and id_ in x]
        if not historical:
            costs = api.get_subscription_costs(id_, "Last365Days")
            datalake_writer.write_json_data(costs, 'test-app', f"{id_}_historical_costs")
        
        else:
            current_date = datetime.now().strftime("%d%m%Y")
            datalake_writer.write_json_data(costs, "test-app", f"{id_}_costs_{current_date}")
        return costs

    tasks = [get_azure_costs(sub) for sub in subscriptions]
    await asyncio.gather(*tasks)
    logger.write_log(client, 'azure', 'INFO', f'Processed costs for {len(subscriptions)} subscriptions') 