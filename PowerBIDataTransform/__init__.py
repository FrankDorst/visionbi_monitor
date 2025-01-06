import logging
import azure.functions as func
import asyncio

from functions.credentials import get_credentials
from functions.clients import initialize_clients
from functions.save_logs import save_logs
from .transform_powerbi import transform_powerbi_data

async def main(powerbiTransform: func.TimerRequest) -> None:
    try:
        secret_client, datalake_writer, logger = get_credentials()
        clients = initialize_clients()['powerbi']
        
        # Execute all client tasks concurrently
        await asyncio.gather(*[
            transform_powerbi_data(client, datalake_writer, logger)
            for client in clients
        ])
        
        save_logs(datalake_writer, "powerbi_transform")
            
    except Exception as e:
        logging.error(f'Error in PowerBI data transformation: {str(e)}')
        if 'datalake_writer' in locals():
            datalake_writer.logger.write_log('system', 'PowerBI Transform', 'ERROR', str(e))
            save_logs(datalake_writer, "powerbi_transform") 