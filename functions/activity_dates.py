from datetime import datetime, timedelta
from typing import List 

def get_todo_dates(client, datalake_writer) -> List[datetime]:
    """Returns dates that need activity data collected"""
    existing_files = datalake_writer.list_files('test')
    todo = []
    today = datetime.now()
    for i in range(29):
        date = today - timedelta(days=i)
        fileName = date.strftime("%d%m%Y")
        if f"{client}_activities_{fileName}" not in existing_files:
            todo.append(date)
    return todo