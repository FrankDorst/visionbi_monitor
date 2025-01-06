from datetime import datetime
import pandas as pd
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from modules.custom_logger import LoggingManager
    from modules.datalake_writer import DataLakeWriter

def merge_new_rows(current_df: pd.DataFrame, silver_df: pd.DataFrame, id_column: str) -> pd.DataFrame:
    """
    Merges rows from silver dataframe that don't exist in current dataframe based on ID column
    """
    if current_df.empty:
        return silver_df
        
    # Find rows in silver that don't exist in current based on ID
    new_rows = silver_df[~silver_df[id_column].isin(current_df[id_column])]
    
    # Concatenate current data with new rows
    return pd.concat([current_df, new_rows], ignore_index=True)

def load_power_bi_data(client: str, writer: "DataLakeWriter", logger: "LoggingManager") -> None:
    """Load Power BI data from silver to gold layer"""
    today = datetime.now().strftime('%d%m%Y')
    silver_path = f'{today}/{client}'
    
    # Define ID columns for each table
    id_columns = {
        'users': 'graphId',
        'activities': 'Id', 
        'user_access': 0, 
        'workspace_content': 0
    }

    # Define table categories and names
    all_tables = {
        'dimensions': ['users', 'workspaces', 'reports', 'datasets'], 
        'facts': ['user_access', 'workspace_content', 'activities', 'azure_spend']
    }

    # Process each table
    for kind, tables in all_tables.items():
        for name in tables:
            try:
                # Read silver data
                dataframe = writer.read_parquet_data('silver', f'{silver_path}/{name}')
                id_column = id_columns.get(name, 'id')
                path_abbreviation = 'dim' if kind == 'dimensions' else 'fact'
                
                try:
                    # Try to read existing gold data
                    current_dataframe = writer.read_parquet_data(
                        'gold', 
                        f"{kind}/{path_abbreviation}_{name}"
                    )
                    df = merge_new_rows(current_dataframe, dataframe, id_column)
                except Exception as e:
                    logger.write_log(
                        client, 
                        'powerbi_load', 
                        'INFO', 
                        f"Creating new table: {name}"
                    )
                    df = dataframe

                # Write to gold layer
                writer.write_parquet_data(
                    df, 
                    'gold', 
                    f"{kind}/{path_abbreviation}_{name}"
                )
                
                logger.write_log(
                    client,
                    'powerbi_load',
                    'INFO',
                    f"Successfully processed {name} table"
                )

            except Exception as e:
                logger.write_log(
                    client,
                    'powerbi_load',
                    'ERROR',
                    f"Error processing {name} table: {str(e)}"
                ) 