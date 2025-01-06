import logging
import azure.functions as func
from datetime import datetime

from functions.credentials import get_credentials
from functions.clients import initialize_clients
from functions.save_logs import save_logs
from .load_powerbi import load_power_bi_data

def main(mytimer: func.TimerRequest) -> None:
    try:
        secret_client, datalake_writer, logger = get_credentials()
        clients = initialize_clients()['powerbi']
        
        # Process each client
        for client in clients:
            load_power_bi_data(client, datalake_writer, logger)
            
        save_logs(datalake_writer, "powerbi_load")
        
        logging.info("PowerBI data loading completed successfully")
            
    except Exception as e:
        error_msg = f'Error in PowerBI data loading: {str(e)}'
        logging.error(error_msg)
        if 'datalake_writer' in locals():
            datalake_writer.logger.write_log('system', 'PowerBI Load', 'ERROR', error_msg)
            save_logs(datalake_writer, "powerbi_load")