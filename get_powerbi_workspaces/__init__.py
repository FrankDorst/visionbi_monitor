import logging
import azure.functions as func
import asyncio
from functions.credentials import get_credentials
from functions.clients import initialize_clients
from .workspaces import process_powerbi_workspaces
from functions.save_logs import save_logs

async def main(powerbiWorkspaces: func.TimerRequest) -> None:
    try:
        secret_client, datalake_writer, logger = get_credentials()
        clients = initialize_clients()['powerbi']
        
        # Execute all client tasks concurrently
        await asyncio.gather(*[
            process_powerbi_workspaces(client, secret_client, datalake_writer, logger)
            for client in clients
        ])
        
        save_logs(datalake_writer, "powerbi_workspaces")
            
    except Exception as e:
        logging.error(f'Error in PowerBI workspaces extraction: {str(e)}')
        if 'datalake_writer' in locals():
            datalake_writer.logger.write_log('system', 'PowerBI Workspaces', 'ERROR', str(e))
            save_logs(datalake_writer, "powerbi_workspaces")