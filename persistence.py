import json
import os
from datetime import datetime

class PersistenceManager:
    """
    Handles saving and loading of transfer history to/from a JSON file.
    """

    def __init__(self, file_path: str = "transfer_history.json"):
        self.file_path = file_path
        # Ensure the file exists
        if not os.path.exists(self.file_path):
            with open(self.file_path, "w") as f:
                json.dump([], f)

    def save_transfer_record(self, record: dict):
        """Append a new transfer record to the history JSON file."""
        try:
            with open(self.file_path, "r") as f:
                data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            data = []

        record["timestamp"] = datetime.now().isoformat(timespec="seconds")
        data.append(record)

        with open(self.file_path, "w") as f:
            json.dump(data, f, indent=4)

    def get_history(self):
        """Load and return all previous transfers."""
        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return []
