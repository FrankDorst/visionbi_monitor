from datetime import datetime
from typing import List, Dict, Any
import pandas as pd
from modules.datalake_writer import DataLakeWriter
from modules.custom_logger import LoggingManager

def fill_user_table(user_access: List, object_type: str, object_id: str, list_of_users: List[Dict]) -> None:
    """Add user access rights to the user_access list"""
    for user in list_of_users:
        access_right = user.get([x for x in user if 'UserAccessRight' in x][0])
        user_access.append([user['graphId'], object_id, access_right, object_type])

def fill_dimension_table(
    workspace_content: List, 
    users: List, 
    all_data: Dict, 
    workspace_id: str, 
    object_type: str, 
    object_data: Dict
) -> None:
    """Process dimension table data and user access rights"""
    try:
        element_id = object_data['id']
    except:
        element_id = object_data['objectId']
        
    fill_user_table(users, object_type, element_id, object_data.get('users', []))

    dimension_data = {key:value for key,value in object_data.items() if isinstance(value, str)}
    try:
        all_data[object_type].append(dimension_data)
    except:
        all_data[object_type] = [dimension_data]

    workspace_content.append([workspace_id, element_id, object_type])

def process_workspace_users(user_access: List, users: List, workspace: Dict) -> None:
    """Process users in a workspace"""
    workspace_id = workspace['id']
    for user in workspace['users']:
        users.append({
            'graphId': user['graphId'],
            'emailAddress': user.get('emailAddress', ''),
            'displayName': user.get('displayName', '')
        })
        access_right = user.get([x for x in user if 'UserAccessRight' in x][0])
        user_access.append([user['graphId'], workspace_id, access_right, 'workspace'])

def process_workspace_objects(workspace_content: List, users: List, all_data: Dict, workspace: Dict) -> None:
    """Process objects (reports, dashboards, etc) in a workspace"""
    workspace_id = workspace['id']
    for key, value in workspace.items():
        if not isinstance(value, list) or key == 'users':
            continue
        for object_data in value:
            object_type = key.rstrip('s')  # Remove trailing 's' to get singular form
            fill_dimension_table(workspace_content, users, all_data, workspace_id, object_type, object_data)

async def transform_powerbi_data(client: str, writer: DataLakeWriter, logger: LoggingManager) -> None:
    """Transform Power BI data from bronze to silver layer"""
    try:
        today = datetime.now().strftime('%d%m%Y')
        silver_path = f'{today}/{client}'

        # Read input data
        input_data = {
            'workspaces': pd.DataFrame(writer.read_json_data('test-app', f'{client}_workspaces_{today}')),
            'activities': pd.DataFrame(writer.read_json_data('test-app', f'{client}_activities_{today}')),
            'workspace_content': writer.read_json_data('bronze', f'{client}_workspace_content_{today}')
        }

        # Process workspace content
        user_access = []
        users = []
        workspace_content = []
        all_data = {}

        # Process each workspace
        for workspace in input_data['workspace_content']:
            for key, value in workspace.items():
                if not isinstance(value, list):
                    continue
                if key == "users":
                    process_workspace_users(user_access, users, workspace)
                else:
                    process_workspace_objects(workspace_content, users, all_data, workspace)

        # Create and deduplicate dataframes
        output_data = {
            'users': pd.DataFrame(users).drop_duplicates(),
            'user_access': pd.DataFrame(user_access).drop_duplicates(),
            'workspaces': input_data['workspaces'],
            'workspace_content': pd.DataFrame(workspace_content),
            'activities': input_data['activities'],
            **{key: pd.DataFrame(value).drop_duplicates() for key, value in all_data.items()}
        }

        # Write all dataframes to silver layer
        for name, df in output_data.items():
            writer.write_parquet_data(df, 'silver', f'{silver_path}/{name}')
            
        logger.write_log(client, 'powerbi', 'INFO', f'Successfully transformed PowerBI data')
            
    except Exception as e:
        logger.write_log(client, 'powerbi', 'ERROR', f'Failed to transform PowerBI data: {str(e)}')
        raise