from datetime import datetime
from typing import List, Dict

class LoggingManager:
    """
    A class to manage logging operations.
    """

    def __init__(self):
        """
        Initialize the LoggingManager with an empty list of logging rows.
        """
        self.logging_rows: List[Dict[str, str]] = []

    def write_log(self, client: str, operation: str, kind: str, text: str) -> None:
        """
        Write a log entry with the specified information.

        Args:
            client (str): The client associated with the log entry.
            operation (str): The operation being performed.
            kind (str): The kind or category of the log entry.
            text (str): The main text content of the log entry.

        Returns:
            None
        """
        current_date = datetime.now()
        log_entry: Dict[str, str] = {
            'client': client,
            'date': current_date.strftime('%d-%m-%Y'),
            'time': current_date.strftime("%H:%M:%S"),
            'operation': operation,
            'kind': kind,
            'text': text
        }
        if kind != "DEBUG":
            print(log_entry.items())
        self.logging_rows.append(log_entry)
