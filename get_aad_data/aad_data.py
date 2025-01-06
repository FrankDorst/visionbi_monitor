from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from azure.keyvault.secrets import SecretClient
    from modules.custom_logger import LoggingManager
    from modules.datalake_writer import DataLakeWriter

async def process_aad_data(client: str, secret_client: "SecretClient", 
                          datalake_writer: "DataLakeWriter", logger: "LoggingManager") -> None:
    """Process AAD data for a client"""
    from modules.graph_api import GraphAPI
    
    api = GraphAPI(client, secret_client, logger)
    users = api.list_all_users()
    
    licensing = []
    for user in users:
        try:
            license_info = await api.get_users_licenses(user['id'])
            if license_info:
                licensing.append({
                    'userId': user['id'],
                    'licenses': license_info
                })
        except Exception as e:
            logger.write_log(client, 'aad', 'ERROR', f'Failed to get licenses for user {user["id"]}: {str(e)}')

    current_date = datetime.now().strftime("%d%m%Y")
    datalake_writer.write_json_data(users, "test-app", f"{client}_users_{current_date}")
    datalake_writer.write_json_data(licensing, "test-app", f"{client}_licensing_{current_date}")
    logger.write_log(client, 'aad', 'INFO', f'Processed {len(users)} users and {len(licensing)} licenses') 