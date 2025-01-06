from modules.datalake_writer import DataLakeWriter
from datetime import datetime 

def save_logs(datalake_writer: "DataLakeWriter", process_name: str) -> None:
    """Save logs from the logger instance to datalake"""
    current_date = datetime.now().strftime("%d%m%Y")
    file_name = f"logging/{current_date}/{process_name}.json"
    datalake_writer.write_json_data(datalake_writer.logger.logging_rows, "test-app", file_name)