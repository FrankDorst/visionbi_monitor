from .microsoft_authentication import MicrosoftAPI
from .custom_logger import LoggingManager
from azure.keyvault.secrets import SecretClient
from typing import List, Dict, Any, Optional
import requests 
from datetime import datetime 


class PowerBIRestAPI(MicrosoftAPI):
    """Class for Power BI REST API interactions."""

    def __init__(self, client: str, secret_client: SecretClient, logger: LoggingManager):
        """
        Initialize the PowerBIRestAPI.

        Args:
            client (str): The client identifier.
            secret_client (SecretClient): Azure Key Vault secret client.
            logger (LoggingManager): Logger instance.
        """
        base_url: str = "https://api.powerbi.com/v1.0/myorg/"
        scope: str = "https://analysis.windows.net/powerbi/api/.default"
        super().__init__(base_url, scope, client, secret_client, logger)

    def get_workspaces(self, top: int = 500, skip: int = 0, data: List[Dict[str, Any]] = []) -> List[Dict[str, Any]]:
        """
        Retrieve a list of workspaces from the Power BI API, handling pagination.

        Args:
            top (int): Maximum number of workspaces to retrieve in one request.
            skip (int): Number of workspaces to skip for pagination.
            data (List[Dict[str, Any]]): List to accumulate retrieved workspaces.

        Returns:
            List[Dict[str, Any]]: List of all workspaces retrieved from the API.

        Raises:
            Exception: If workspace retrieval fails.
        """
        try:
            response: requests.Response = self.make_request('GET', f'admin/groups?$top={top}&$skip={skip}')
            result: List[Dict[str, Any]] = response.json().get('value', [])
            data.extend(result)

            self.logger.write_log(self.client, 'Get Workspaces', 'DEBUG', f'Retrieved {len(result)} workspaces (skip={skip}). Total workspaces: {len(data)}')

            if len(result) == top:
                return self.get_workspaces(top, skip + top, data)
            
            self.logger.write_log(self.client, 'Get Workspaces', 'INFO', f'Successfully retrieved a total of {len(data)} workspaces')
            return data
        except Exception as e:
            self.logger.write_log(self.client, 'Get Workspaces', 'ERROR', f'Failed to get workspaces: {str(e)}')
            raise

    def post_workspace_scan(self, workspace_ids: List[str]) -> List[str]:
        """
        Initiate a scan for a list of workspaces.

        Args:
            workspace_ids (List[str]): List of workspace IDs to be scanned.

        Returns:
            List[str]: List of scan IDs corresponding to the initiated scans.

        Raises:
            requests.RequestException: If workspace scan initiation fails.
        """
        workspace_scan_ids: List[str] = []
        url: str = "admin/workspaces/getInfo?lineage=True&datasourceDetails=True&datasetSchema=True&datasetExpressions=True&getArtifactUsers=True"
        try:
            for i in range(0, len(workspace_ids), 100):
                body: Dict[str, List[str]] = {"workspaces": workspace_ids[i:i + 100]}
                try:
                    response: requests.Response = self.make_request('POST', url, payload=body)
                except requests.RequestException as e:
                    self.logger.write_log(self.client, 'GET workspace scan', 'ERROR', f'Error with normal endpoint: {e}')
                    new_url: str = "admin/workspaces/getInfo?getArtifactUsers=True"
                    response = self.make_request('POST', new_url, payload=body)

                workspace_scan_ids.append(response.json()['id'])

            self.logger.write_log(self.client, 'Post Workspace Scan', 'INFO', f'Successfully initiated {len(workspace_scan_ids)} workspace scans. Total workspaces: {len(workspace_ids)}')
            return workspace_scan_ids
        except requests.RequestException as e:
            self.logger.write_log(self.client, 'Post Workspace Scan', 'ERROR', f'Failed to post workspace scan: {str(e)}')
            raise

    def get_workspace_scans(self, scan_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Retrieve the scan results for a list of workspace scan IDs.

        Args:
            scan_ids (List[str]): List of scan IDs to retrieve results for.

        Returns:
            List[Dict[str, Any]]: List of dictionaries containing the scan results for each workspace.

        Raises:
            requests.RequestException: If scan result retrieval fails.
        """

        data: List[Dict[str, Any]] = []
        for scan_id in scan_ids:
            try:
                result: Dict[str, Any] = self.make_request('GET', f'admin/workspaces/scanResult/{scan_id}').json()
                data.extend(result)
                self.logger.write_log(self.client, 'Get Workspace Scans', 'DEBUG', f'Retrieved scan result for scan ID {scan_id}. Results count: {len(result)}')
            except requests.RequestException as e:
                self.logger.write_log(self.client, 'Get Workspace Scans', 'ERROR', f'Failed to get scan result for scan ID {scan_id}: {str(e)}')
                raise

        self.logger.write_log(self.client, 'Get Workspace Scans', 'INFO', f'Successfully retrieved scan results for {len(scan_ids)} workspace(s). Total results: {len(data)}')
        return data
    
    def get_tenant_activities(self, date: datetime, continuation_token: Optional[str] = None, data: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        Retrieve tenant activities for a specific date, handling pagination with continuation tokens.

        Args:
            date (datetime): The date for which to retrieve activities.
            continuation_token (Optional[str]): Token for continuing a previous request.
            data (Optional[List[Dict[str, Any]]]): Accumulated data from previous calls.

        Returns:
            List[Dict[str, Any]]: List of dictionaries containing tenant activity data.

        Raises:
            requests.RequestException: If tenant activity retrieval fails.
        """
        try:
            if data is None:
                data = []
                self.logger.write_log(self.client, 'Get Tenant Activities', 'DEBUG', 'Initializing new data list')

            if not continuation_token:
                start_time: str = date.strftime("%Y-%m-%dT00:00:00.000Z")
                end_time: str = date.strftime("%Y-%m-%dT23:59:59.000Z")
                endpoint_url: str = f"admin/activityevents?startDateTime='{start_time}'&endDateTime='{end_time}'&$filter=Activity eq 'ViewReport'"
                self.logger.write_log(self.client, 'Get Tenant Activities', 'INFO', f'Fetching activities for date: {date.date()}')
            else:
                endpoint_url: str = f"admin/activityevents?continuationToken='{continuation_token}'"
                self.logger.write_log(self.client, 'Get Tenant Activities', 'DEBUG', f'Using continuation token for pagination. Current data count: {len(data)}')

            response: Dict[str, Any] = self.make_request("GET", endpoint=endpoint_url).json()
            self.logger.write_log(self.client, 'Get Tenant Activities', 'DEBUG', 'Successfully received response from API')

            new_activities = response.get('activityEventEntities', [])
            data.extend(new_activities)
            
            self.logger.write_log(self.client, 'Get Tenant Activities', 'INFO', f'Added {len(new_activities)} activities to data. Total activities: {len(data)}')

            token: Optional[str] = response.get('continuationToken')
            if token:
                self.logger.write_log(self.client, 'Get Tenant Activities', 'DEBUG', f'Continuation token found, making recursive call. Current data count: {len(data)}')
                return self.get_tenant_activities(date, token, data)
            else:
                self.logger.write_log(self.client, 'Get Tenant Activities', 'INFO', f'Completed fetching all activities for date: {date.date()}. Total activities: {len(data)}')
                return data

        except requests.RequestException as e:
            self.logger.write_log(self.client, 'Get Tenant Activities', 'ERROR', f'Failed to get tenant activities: {str(e)}')
            raise


