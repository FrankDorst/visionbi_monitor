from modules.microsoft_api import MicrosoftAPI
from azure.keyvault.secrets import SecretClient
from modules.custom_logger import LoggingManager
from typing import Optional, List, Dict, Any
import requests

class GraphAPI(MicrosoftAPI):
    """Class for Microsoft Graph API interactions."""

    def __init__(self, client: str, secret_client: SecretClient, logger: LoggingManager):
        """
        Initialize the GraphAPI.

        Args:
            client (str): The client identifier.
            secret_client (SecretClient): Azure Key Vault secret client.
            logger (LoggingManager): Logger instance.
        """
        base_url: str = "https://graph.microsoft.com/v1.0/"
        scope: str = "https://graph.microsoft.com/.default"
        super().__init__(base_url, scope, client, secret_client, logger)

    def list_all_users(self, next_link: Optional[str] = None, data: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        Retrieve a list of all users recursively.

        Args:
            next_link (Optional[str]): The URL for the next page of results.
            data (Optional[List[Dict[str, Any]]]): Accumulated user data from previous calls.

        Returns:
            List[Dict[str, Any]]: List containing all user data.

        Raises:
            requests.RequestException: If user retrieval fails.
        """
        if data is None:
            data = []

        try:
            self.logger.write_log(self.client, 'List All Users', 'INFO', f'Retrieving users. Current user count: {len(data)}')
            url = next_link.split("v1.0/")[-1] if next_link else '/users'
            response: requests.Response = self.make_request("GET", url)
            response_json = response.json()

            if 'value' in response_json:
                new_users = response_json['value']
                data.extend(new_users)
                self.logger.write_log(self.client, 'List All Users', 'DEBUG', f'Retrieved {len(new_users)} users. Total users: {len(data)}')

            if '@odata.nextLink' in response_json:
                return self.list_all_users(response_json['@odata.nextLink'], data)
            else:
                self.logger.write_log(self.client, 'List All Users', 'INFO', f'Successfully retrieved all users. Total users: {len(data)}')
                return data
        except requests.RequestException as e:
            self.logger.write_log(self.client, 'List All Users', 'ERROR', f'Failed to retrieve users: {str(e)}')
            raise
    
    def get_users_licenses(self, user_id: str) -> Dict[str, Any]:
        """
        Retrieve license details for a specific user.

        Args:
            user_id (str): The ID of the user to query.

        Returns:
            Dict[str, Any]: Dictionary containing user license data.

        Raises:
            requests.RequestException: If license retrieval fails.
        """
        try:
            self.logger.write_log(self.client, 'Get User Licenses', 'INFO', f'Retrieving licenses for user: {user_id}')
            response: requests.Response = self.make_request("GET", f'users/{user_id}/licenseDetails')
            license_data = response.json()
            self.logger.write_log(self.client, 'Get User Licenses', 'INFO', f'Successfully retrieved licenses for user: {user_id}. Number of licenses: {len(license_data.get("value", []))}')
            return license_data
        except requests.RequestException as e:
            self.logger.write_log(self.client, 'Get User Licenses', 'ERROR', f'Failed to retrieve licenses for user {user_id}: {str(e)}')
            raise