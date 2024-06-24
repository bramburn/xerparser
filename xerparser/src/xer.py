# xerparser
# xer.py

from pathlib import Path
from typing import BinaryIO

import numpy as np
import pandas as pd

from xerparser import CODEC, file_reader, parser
from xerparser.schemas.task import calculate_completion, calculate_duration, calculate_remaining_days
from xerparser.schemas.taskpred import calculate_lag_days


class Xer:
    """
    A class to represent the schedule data included in a .xer file.
    """

    # class variables
    CODEC = CODEC

    def __init__(self, xer_file_contents: str) -> None:
        self.project_df, self.task_df, self.taskpred_df, self.projwbs_df, self.calendar_df, self.account_df = self._parse_xer_data(
            xer_file_contents)

    def _parse_xer_data(self, xer_file_contents: str) -> tuple[
        pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Parse the XER file contents and return the table data as DataFrames."""
        if xer_file_contents.startswith("ERMHDR"):
            xer_data = parser(xer_file_contents)
            tasks = xer_data.get('TASK', None)
            if tasks is not None:
                # Convert non-empty strings in 'act_start_date' and 'act_end_date' to datetime
                tasks['act_start_date'] = pd.to_datetime(tasks['act_start_date'].replace('', np.nan),
                                                         format='%Y-%m-%d %H:%M', errors='coerce')
                tasks['act_end_date'] = pd.to_datetime(tasks['act_end_date'].replace('', np.nan),
                                                       format='%Y-%m-%d %H:%M', errors='coerce')
                tasks['progress'] = tasks.apply(calculate_completion, axis=1)
                tasks['duration'] = tasks.apply(calculate_duration, axis=1)
                tasks['remaining_days'] = tasks.apply(calculate_remaining_days, axis=1)
                tasks['early_start'] = np.nan
                tasks['early_finish'] = np.nan
                tasks['late_start'] = np.nan
                tasks['late_finish'] = np.nan

            task_pred = xer_data.get('TASKPRED', None)
            if task_pred is not None:
                task_pred['lag_days'] = task_pred.apply(calculate_lag_days,axis=1)


            return (
                xer_data.get('PROJECT', None),
                tasks,
                task_pred,
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
