

import json
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient
from datetime import datetime
from typing import Any, Dict


class DataLakeWriter:
    """
    A class that handles reading and writing JSON data to Azure Data Lake Storage (ADLS) Gen2.

    Attributes:
        client (DataLakeServiceClient): The client used to interact with ADLS Gen2.
        logger (LoggingManager): A logger instance to log operations.
    """

    def __init__(self, connection_string: str, logger: Any):
        """
        Initializes the DataLakeWriter with the connection string and logger instance.

        Args:
            connection_string (str): The connection string for Azure Data Lake Storage Gen2.
            logger (LoggingManager): An instance of the LoggingManager to store logs.
        """
        self.client = DataLakeServiceClient.from_connection_string(connection_string)
        self.logger = logger

    def write_json_data(self, data: Dict[str, Any], file_system: str, file_name: str) -> None:
        """
        Writes JSON data to the specified file in the ADLS Gen2 file system.

        Args:
            data (Dict[str, Any]): The JSON data to be written.
            file_system (str): The name of the ADLS Gen2 file system.
            file_name (str): The name of the file where the data will be written.

        Raises:
            Exception: If there's an error writing the data to ADLS.
        """
        try:
            # Get file system client
            file_system_client: FileSystemClient = self.client.get_file_system_client(file_system)

            # Convert data to JSON string and bytes
            json_data = json.dumps(data, indent=4)
            json_bytes = json_data.encode('utf-8')

            # Create file and write data to it
            file_client = file_system_client.create_file(file_name)
            file_client.append_data(data=json_bytes, offset=0, length=len(json_bytes))
            file_client.flush_data(len(json_bytes))

            # Log successful write operation
            self.logger.write_log('datalake_writer', 'DEBUG', 'Write data', f'Data written to {file_name} in {file_system}')
        except Exception as e:
            # Log any exceptions that occur during the write operation
            self.logger.write_log('datalake_writer', 'ERROR', 'Write data', f'Error writing data to {file_name}: {str(e)}')
            raise

    def read_json_data(self, file_system: str, file_name: str) -> Dict[str, Any]:
        """
        Reads JSON data from the specified file in the ADLS Gen2 file system.

        Args:
            file_system (str): The name of the ADLS Gen2 file system.
            file_name (str): The name of the file to read data from.

        Returns:
            Dict[str, Any]: The parsed JSON data read from the file.

        Raises:
            Exception: If there's an error reading the data from ADLS.
        """
        try:
            # Get file system client and file client
            file_system_client: FileSystemClient = self.client.get_file_system_client(file_system)
            file_client = file_system_client.get_file_client(file_name)

            # Download the file content
            download_stream = file_client.download_file()
            file_content = download_stream.readall().decode('utf-8')

            # Parse and return JSON data
            data = json.loads(file_content)

            # Log successful read operation
            self.logger.write_log('datalake_writer', 'DEBUG', 'Read data', f'Data read from {file_name} in {file_system}')
            
            return data
        except Exception as e:
            # Log any exceptions that occur during the read operation
            self.logger.write_log('datalake_writer', 'ERROR', 'Read data', f'Error reading data from {file_name}: {str(e)}')
            raise

    def list_files(self, file_system: str):
        """
        Lists all files in the specified ADLS Gen2 file system.

        Args:
            file_system (str): The name of the ADLS Gen2 file system.

        Returns:
            List[str]: A list of file paths within the specified file system.

        Raises:
            Exception: If there's an error retrieving the file list from ADLS.
        """
        try:
            # Get the file system client
            file_system_client: FileSystemClient = self.client.get_file_system_client(file_system)
            
            # List all paths (files and directories)
            paths = file_system_client.get_paths()
            
            # Collect file paths into a list
            file_list = [path.name for path in paths if not path.is_directory]
            
            # Log the successful listing of files
            self.logger.write_log('datalake_writing', 'DEBUG', 'List files', f'Files listed in {file_system}: {file_list}')
            
            return file_list
        except Exception as e:
            # Log any exceptions that occur during the listing operation
            self.logger.write_log('datalake_writing', 'ERROR', 'List files', f'Error listing files in {file_system}: {str(e)}')
            raise




