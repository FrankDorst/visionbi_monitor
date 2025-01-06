from datetime import datetime 
import concurrent.futures
import pandas as pd 

today = datetime.now().strftime('%d%m%Y')





def fill_user_table(user_access, object_type, object_id, list_of_users):
    for user in list_of_users:
        access_right = user.get([x for x in user if 'UserAccessRight' in x][0])
        user_access.append([user['graphId'], object_id, access_right, object_type])


def fill_dimension_table(workspace_content, users, all_data, workspace_id, object_type, object_data):
    try:
        element_id = object_data['id']
    except:
        element_id = object_data['objectId']
    fill_user_table(users, object_type, element_id, object_data.get('users', []))

    dimension_data = {key:value for key,value in object_data.items() if type(value) == str}
    try:
        all_data[object_type].append(dimension_data)
    except:
        all_data[object_type] = [dimension_data]

    workspace_content.append([workspace_id, element_id, object_type])


def transformation(client_config, datalake_writer, sparkManager, secretManager):
    """Process all clients per service type in parallel"""
    for service, client_list in client_config.items():
        print(f"Processing {service} data for clients: {client_list}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            if service == 'powerbi':
                futures = [
                    executor.submit(process_single_client, client, datalake_writer, secretManager)
                    for client in client_list
                ]
            #elif service == 'azure':
                # Alleen Azure subscription data verwerken voor Azure clients
            #    futures = [
            #        executor.submit(process_azure_subscription_only, client, datalake_writer, secretManager)
            #        for client in client_list
            #    ]
            # Je kunt hier meer services toevoegen indien nodig
            
            concurrent.futures.wait(futures)

def process_azure_subscription_only(client: str, datalake_writer, secret_manager) -> None:
    """Process only Azure subscription data for a client"""
    try:
        # Process Azure subscriptions
        azure_subs = [x.name for x in secret_manager.list_properties_of_secrets() 
                     if client in x.name and "sub" in x.name]

        for sub_name in azure_subs:
            process_azure_subscription(client, sub_name, datalake_writer, secret_manager)
    except Exception as e:
        print(f"Error processing Azure data for client {client}: {str(e)}")
        raise

def process_single_client(client: str, datalake_writer, secret_manager) -> None:
    """Process data for a single client"""
    try:
        # Read data
        workspace_data = pd.DataFrame(datalake_writer.read_json_data('test-app', f'{client}_workspaces_{today}'))
        activities_data = pd.DataFrame(datalake_writer.read_json_data('test-app', f'{client}_activities_{today}'))
        workspace_content_data = datalake_writer.read_json_data('test-app', f'{client}_workspace_content_{today}')

        # Initialize collections
        user_access = []
        users = []
        workspace_content = []
        all_data = {}

        # Process workspace content
        for workspace in workspace_content_data:
            for key, value in workspace.items():
                if isinstance(value, list):
                    if key == "users":
                        fill_user_table(user_access, 'workspace', workspace['id'], value)
                        users.extend(value)
                    else:
                        for object_ in value:
                            fill_dimension_table(workspace_content, user_access, all_data, 
                                              workspace['id'], key, object_)

        # Convert to DataFrames
        df_workspace_content = pd.DataFrame(workspace_content, 
                                          columns=["workspaceID", "objectID", "objectType"])
        df_user_access = pd.DataFrame(user_access, 
                                    columns=["userID", "objectID", "accessType"])
        df_users = pd.DataFrame(users)

        # Create dimension dataframes
        df_object_dimension_data = {
            key: pd.DataFrame(value) 
            for key, value in all_data.items() 
            if key in ["reports", "datasets"]
        }

        # Write to silver layer
        datalake_writer.write_parquet_data(
            df_workspace_content,
            'silver',
            f'{client}_workspace_content'
        )

        for object_key, dataframe in df_object_dimension_data.items():
            datalake_writer.write_parquet_data(
                dataframe,
                'silver', 
                f'{client}_{object_key}'
            )

        datalake_writer.write_parquet_data(
            df_users,
            'silver',
            f'{client}_users'
        )

        datalake_writer.write_parquet_data(
            df_user_access,
            'silver',
            f'{client}_user_access'
        )

        # Process Azure subscriptions
        azure_subs = [x.name for x in secret_manager.list_properties_of_secrets() 
                     if client in x.name and "sub" in x.name]

        for sub_name in azure_subs:
            process_azure_subscription(client, sub_name, datalake_writer, secret_manager)

    except Exception as e:
        print(f"Error processing client {client}: {str(e)}")
        raise

def process_azure_subscription(client: str, sub_name: str, 
                             datalake_writer, secret_manager) -> None:
    """Process Azure subscription data for a client"""
    try:
        sub = secret_manager.get_secret(sub_name).value
        fileName = sub.split('-')[-1]

        try:
            # Try to read existing data
            historical_data = pd.DataFrame(
                datalake_writer.read_json_data('silver', f"{client}_{fileName}_spend")
            )
        except:
            # If no existing data, read from historical
            try:
                historical_data = datalake_writer.read_json_data(
                    'bronze', 
                    f'{client}_azure_{fileName}_historical'
                )
                h_data = historical_data['properties']['rows']
                historical_data = pd.DataFrame(h_data)
                
                datalake_writer.write_json_data(
                    historical_data.to_dict(orient='records'),
                    'silver',
                    f"{client}_{fileName}_spend"
                )
            except:
                historical_data = pd.DataFrame()

        # Process today's data
        try:
            yesterdays_data = datalake_writer.read_json_data(
                'bronze', 
                f'{client}_azure_{fileName}_{today}'
            )
            y_data = yesterdays_data['properties']['rows']
            df = pd.DataFrame(y_data)

            # Append new data
            if not historical_data.empty:
                df = pd.concat([historical_data, df], ignore_index=True)

            datalake_writer.write_json_data(
                df.to_dict(orient='records'),
                'silver',
                f"{client}_{fileName}_spend"
            )
        except Exception as e:
            print(f"Error processing yesterday's data for {client}/{fileName}: {str(e)}")

    except Exception as e:
        print(f"Error processing subscription {sub_name}: {str(e)}")



from functions.credentials import get_credentials
from functions.clients import initialize_clients


# Main execution
if __name__ == "__main__":
    secret, writer, logger = get_credentials()
    client_config = initialize_clients()
    
    spark = None
    transformation(client_config, writer, spark, secret)
