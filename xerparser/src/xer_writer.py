# xerwriter.py
from typing import List, Dict


class XerWriter:
    """
    A class to represent the schedule data and write it to a .xer file.
    """

    def __init__(self, data: Dict[str, List[Dict]]) -> None:
        self.data = data
        self._build_xer_content()

    def _build_xer_content(self) -> None:
        """
        Builds the content of the .xer file from the provided data.
        """
        self.xer_content = "%T\tERMHDR\n"
        self.xer_content += "\t".join(self.data["ERMHDR"]) + "\n"

        for table_name, rows in self.data.items():
            if table_name == "ERMHDR":
                # Skip the header table as it's already added
                continue

            self.xer_content += f"%T\t{table_name}\n"
            if rows:
                # Assuming the first row's keys are the column names
                column_names = list(rows[0].keys())
                self.xer_content += "\t" + "\t".join(column_names) + "\n"
                for row in rows:
                    self.xer_content += "\t" + "\t".join(str(row[col]) for col in column_names) + "\n"
            else:
                # Add an empty row if the table has no data
                self.xer_content += "\n"

    def write(self, file_path: str) -> None:
        """
        Writes the .xer content to a file.

        Args:
            file_path (str): Path to the .xer file.
        """
        with open(file_path, 'w') as file:
            file.write(self.xer_content)