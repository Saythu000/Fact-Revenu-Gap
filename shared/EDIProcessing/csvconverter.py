import os
import csv
from typing import Dict, List, Union, Any

class CSVConverter:
    def __init__(self):
        pass

    def converter(self, data, output_file_path):
        # 1. Keep the file path separate, and extract the folder path safely
        folder_path = os.path.dirname(output_file_path)
        
        # 2. Check the folder path
        if folder_path and not os.path.exists(folder_path):
            raise FileNotFoundError(f"The target directory does not exist: {folder_path}")

        data_list = data if isinstance(data, list) else [data]
        if not data_list or not data_list[0]:
            return output_file_path
        
        headers = data_list[0].keys()
        
        # 3. Open the actual FILE path, not the folder path
        with open(output_file_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file, delimiter=",", quoting=csv.QUOTE_MINIMAL)
            writer.writerow(headers)
            
            for record in data_list:
                row = [str(record.get(header, "")).strip() for header in headers]
                writer.writerow(row)

        return output_file_path
