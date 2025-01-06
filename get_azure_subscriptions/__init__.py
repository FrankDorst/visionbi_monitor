import logging

import azure.functions as func

import asyncio

from functions.credentials import get_credentials

from functions.clients import initialize_clients

from .azure_data import process_azure_data

from functions.save_logs import save_logs



async def main(azureData: func.TimerRequest) -> None:

    try:

        secret_client, datalake_writer, logger = get_credentials()

        clients = initialize_clients()['azure']

        

        # Execute all client tasks concurrently

        await asyncio.gather(*[

            process_azure_data(client, secret_client, datalake_writer, logger)

            for client in clients

        ])

        

        save_logs(datalake_writer, "azure_data")

            

    except Exception as e:

        logging.error(f'Error in Azure data extraction: {str(e)}')

        if 'datalake_writer' in locals():

            datalake_writer.logger.write_log('system', 'Azure Data', 'ERROR', str(e))

            save_logs(datalake_writer, "azure_data")
