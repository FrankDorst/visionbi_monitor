import requests 
from azure.keyvault.secrets import SecretClient
from .custom_logger import LoggingManager 
from typing import Optional, Dict, List
from datetime import datetime, timedelta

class MicrosoftAPI:
    """Base class for Microsoft API interactions."""

    def __init__(self, base_url: str, scope: str, client: str, secret_manager: SecretClient, logger: LoggingManager):
        """
        Initialize the MicrosoftAPI.

        Args:
            base_url (str): The base URL for API requests.
            scope (str): The scope for authentication.
            client (str): The client identifier.
            secret_manager (SecretClient): Azure Key Vault secret client.
            logger (LoggingManager): Logger instance.
        """
        self.base_url: str = base_url
        self.scope: str = scope
        self.client: str = client

        try:
            self.tenant_id: str = secret_manager.get_secret(f"{client}-tenant-id").value
            self.client_id: str = secret_manager.get_secret(f"{client}-client-id").value
            self.client_secret: str = secret_manager.get_secret(f"{client}-client-secret").value
        except Exception as e:
            logger.write_log(client, 'Initialization', 'ERROR', f'Failed to retrieve secrets: {str(e)}')
            raise
        
        self.token: Optional[str] = None
        self.expiration_time: Optional[datetime] = None
        self.logger: LoggingManager = logger

    def _get_access_token(self) -> str:
        """
        Retrieve an OAuth2 access token using client credentials.

        Returns:
            str: The access token.

        Raises:
            requests.RequestException: If token retrieval fails.
        """
        url: str = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        body: Dict[str, str] = {
            "scope": self.scope,
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }

        try:
            headers: Dict[str, str] = {"Content-Type": "application/x-www-form-urlencoded"}
            response: requests.Response = requests.post(url, data=body, headers=headers)
            response.raise_for_status()
            self.token = response.json()['access_token']
            self.expiration_time = datetime.now() + timedelta(minutes=15)

            self.logger.write_log(self.client, 'Token', 'INFO', f'Access token retrieved successfully. Token length: {len(self.token)}')
            return self.token
        except requests.RequestException as e:
            self.logger.write_log(self.client, 'Token', 'ERROR', f'Failed to retrieve access token: {str(e)}')
            raise

    def _get_headers(self) -> Dict[str, str]:
        """
        Generate authorization headers for API requests.

        Returns:
            Dict[str, str]: Headers dictionary.
        """
        try:
            if not self.token or datetime.now() >= self.expiration_time:
                self._get_access_token()
            return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        except Exception as e:
            self.logger.write_log(self.client, 'Headers', 'ERROR', f'Failed to generate headers: {str(e)}')
            raise

    def make_request(self, method: str, endpoint: str, params: Optional[Dict] = None, payload: Optional[Dict] = None) -> requests.Response:
        """
        Make a dynamic request to the Microsoft API.

        Args:
            method (str): HTTP method (e.g., 'GET', 'POST', 'PUT', 'DELETE').
            endpoint (str): API endpoint to call.
            params (Optional[Dict]): Query parameters.
            payload (Optional[Dict]): Request payload.

        Returns:
            requests.Response: The API response.

        Raises:
            requests.RequestException: If the API request fails.
        """
        url: str = f"{self.base_url}{endpoint}"
        headers: Dict[str, str] = self._get_headers()

        try:
            response: requests.Response = requests.request(method, url, headers=headers, params=params, json=payload)
            response.raise_for_status()
            self.logger.write_log(self.client, 'API Request', 'INFO', f'Successfully made {method} request to {url}. Response status code: {response.status_code}')
            return response
        except requests.RequestException as e:
            self.logger.write_log(self.client, 'API Request', 'ERROR', f'Failed to make {method} request to {url}: {str(e)}')
            raise
