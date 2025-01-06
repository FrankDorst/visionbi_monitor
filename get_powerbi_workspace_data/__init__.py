import logging

import azure.functions as func

import asyncio

from functions.credentials import get_credentials

from functions.clients import initialize_clients

from .content import process_powerbi_content

from functions.save_logs import save_logs



async def main(powerbiContent: func.TimerRequest) -> None:

    try:

        secret_client, datalake_writer, logger = get_credentials()

        clients = initialize_clients()['powerbi']

        

        # Execute all client tasks concurrently

        await asyncio.gather(*[

            process_powerbi_content(client, secret_client, datalake_writer, logger)

            for client in clients

        ])

            

        save_logs(datalake_writer, "powerbi_content")

            

    except Exception as e:

        logging.error(f'Error in PowerBI content extraction: {str(e)}')

        if 'datalake_writer' in locals():

            datalake_writer.logger.write_log('system', 'PowerBI Content', 'ERROR', str(e))

            save_logs(datalake_writer, "powerbi_content")
