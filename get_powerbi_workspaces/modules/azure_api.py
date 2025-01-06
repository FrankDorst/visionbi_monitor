from modules.microsoft_api import MicrosoftAPI
from azure.keyvault.secrets import SecretClient
from modules.custom_logger import LoggingManager
import requests 
from typing import Dict, Any

class AzureRestAPI(MicrosoftAPI):
    """Class for Azure REST API interactions."""

    def __init__(self, client: str, secret_client: SecretClient, logger: LoggingManager):
        """
        Initialize the AzureRestAPI.

        Args:
            client (str): The client identifier.
            secret_client (SecretClient): Azure Key Vault secret client.
            logger (LoggingManager): Logger instance.
        """
        base_url: str = "https://management.azure.com/"
        scope: str = base_url + ".default"
        super().__init__(base_url, scope, client, secret_client, logger)

    def get_subscription_costs(self, subscription_id: str, timeframe: str) -> Dict[str, Any]:
        """
        Retrieve subscription costs for a given subscription ID and timeframe.

        Args:
            subscription_id (str): The ID of the subscription to query.
            timeframe (str): The timeframe for which to retrieve costs.

        Returns:
            Dict[str, Any]: Dictionary containing the subscription cost data.

        Raises:
            requests.RequestException: If subscription cost retrieval fails.
        """
        try:
            self.logger.write_log(self.client, 'Get Subscription Costs', 'INFO', f'Fetching costs for subscription: {subscription_id}')
            
            payload: Dict[str, Any] = {  
                "type": "Usage",
                "timeframe": timeframe,
                "dataset": {
                    "granularity": "Daily",
                    "aggregation": {
                        "totalCost": {
                            "name": "Cost",
                            "function": "Sum"
                        }
                    },
                    "grouping": [
                        {"type": "Dimension", "name": "ResourceGroupName"},
                        {"type": "Dimension", "name": "ResourceType"},
                        {"type": "Dimension", "name": "MeterCategory"}
                    ]
                }
            }
            endpoint: str = f'subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2021-10-01' 

            self.logger.write_log(self.client, 'Get Subscription Costs', 'DEBUG', 'Making API request')
            response: requests.Response = self.make_request("POST", endpoint, payload=payload)
            
            cost_data = response.json()
            self.logger.write_log(self.client, 'Get Subscription Costs', 'INFO', f'Successfully retrieved subscription costs. Data points: {len(cost_data.get("properties", {}).get("rows", []))}')
            return cost_data
        except requests.RequestException as e:
            self.logger.write_log(self.client, 'Get Subscription Costs', 'ERROR', f'Failed to get subscription costs: {str(e)}')
            raise