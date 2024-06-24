# xerparser
# xer.py

from pathlib import Path
from typing import BinaryIO

import pandas as pd

from xerparser import CODEC, file_reader, parser

class Xer:
    """
    A class to represent the schedule data included in a .xer file.
    """

    # class variables
    CODEC = CODEC

    def __init__(self, xer_file_contents: str) -> None:
        self.project_df, self.task_df, self.taskpred_df, self.projwbs_df, self.calendar_df, self.account_df = self._parse_xer_data(xer_file_contents)

    def _parse_xer_data(self, xer_file_contents: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Parse the XER file contents and return the table data as DataFrames."""
        if xer_file_contents.startswith("ERMHDR"):
            xer_data = parser(xer_file_contents)
            return (
                xer_data.get('PROJECT', None),
                xer_data.get('TASK', None),
                xer_data.get('TASKPRED', None),
                xer_data.get('PROJWBS', None),
                xer_data.get('CALENDAR', None),
                xer_data.get('ACCOUNT', None)
            )
        else:
            raise ValueError("ValueError: invalid XER file")

    @classmethod
    def reader(cls, file: Path | str | BinaryIO) -> "Xer":
        """
        Create an Xer object directly from a .XER file.

        Files can be passed as a:
            * Path directory (str or pathlib.Path)
            * Binary file (from requests, Flask, FastAPI, etc...)

        """
        file_contents = file_reader(file)
        return cls(file_contents)


def generate_xer_contents(xer: Xer) -> str:
    """Generate the updated XER file contents from the modified DataFrames."""
    xer_contents = "ERMHDR\t" + "\t".join([str(x) for x in xer.tables['ERMHDR'].iloc[0]]) + "\n"

    for table_name, df in xer.tables.items():
        if table_name != 'ERMHDR':
            xer_contents += f"%T\t{table_name}\n"
            xer_contents += "\t".join(df.columns) + "\n"
            for _, row in df.iterrows():
                xer_contents += "\t".join([str(x) for x in row]) + "\n"

    return xer_contents