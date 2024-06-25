from pathlib import Path
from typing import BinaryIO
from datetime import datetime, time
import json

import numpy as np
import pandas as pd

from local.libs.calendar_parser import CalendarParser
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
        self.tables = self._parse_xer_data(xer_file_contents)
        self.project_df = self.tables.get('PROJECT', None)
        self.task_df = self.tables.get('TASK', None)
        self.taskpred_df = self.tables.get('TASKPRED', None)
        self.projwbs_df = self.tables.get('PROJWBS', None)
        self.calendar_df = self.tables.get('CALENDAR', None)
        self.account_df = self.tables.get('ACCOUNT', None)

    def _parse_xer_data(self, xer_file_contents: str) -> dict[str, pd.DataFrame]:
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
                task_pred['lag_days'] = task_pred.apply(calculate_lag_days, axis=1)

            # Process CALENDAR table using CalendarParser
            calendar_df = xer_data.get('CALENDAR', None)
            if calendar_df is not None:
                calendar_parser = CalendarParser(calendar_df)
                calendar_parser.parse_calendars()

                # Add parsed calendar data back to the DataFrame
                calendar_df['parsed_workdays'] = calendar_df['clndr_id'].map(
                    lambda x: json.dumps(self._convert_times_to_strings(calendar_parser.calendars[x]['workdays']))
                )
                calendar_df['parsed_exceptions'] = calendar_df['clndr_id'].map(
                    lambda x: json.dumps({str(k): self._convert_times_to_strings(v) for k, v in
                                          calendar_parser.calendars[x]['exceptions'].items()})
                )

                xer_data['CALENDAR'] = calendar_df

            return xer_data
        else:
            raise ValueError("ValueError: invalid XER file")

    @staticmethod
    def _convert_times_to_strings(data):
        if isinstance(data, dict):
            return {k: Xer._convert_times_to_strings(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [Xer._convert_times_to_strings(item) for item in data]
        elif isinstance(data, tuple) and all(isinstance(x, time) for x in data):
            return [t.strftime('%H:%M') for t in data]
        else:
            return data

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

    def update_last_recalc_date(self, split_date: datetime) -> None:
        """
        Update the project's last_recalc_date field to the split_date.

        Args:
            split_date (datetime): The date to set as the new last_recalc_date
        """
        if self.project_df is not None and 'last_recalc_date' in self.project_df.columns:
            self.project_df['last_recalc_date'] = split_date.strftime('%Y-%m-%d %H:%M')

    def generate_xer_contents(self) -> str:
        """Generate the updated XER file contents from the modified DataFrames."""
        xer_contents = ""

        # Handle ERMHDR specially
        if 'ERMHDR' in self.tables and not self.tables['ERMHDR'].empty:
            # If ERMHDR exists, use it and replace empty values with ''
            ermhdr_row = self.tables['ERMHDR'].iloc[0].fillna('')
            xer_contents += "ERMHDR\t" + "\t".join([str(x) for x in ermhdr_row]) + "\n"
        else:
            # If ERMHDR is missing or empty, create a minimal header based on the provided format
            xer_contents += "ERMHDR\t16.2\t2023-04-14\tProject\tUSER\tUSERNAME\tdbxDatabaseNoName\tProject Management\tUSD\n"

        for table_name, df in self.tables.items():
            if table_name != 'ERMHDR' and not df.empty:
                xer_contents += f"%T\t{table_name}\n"
                xer_contents += "%F\t" + "\t".join(df.columns) + "\n"
                for _, row in df.iterrows():
                    xer_contents += "%R\t" + "\t".join([self._format_value(x) for x in row]) + "\n"

        return xer_contents

    def _format_value(self, value):
        """Format values for XER output, handling datetime objects."""
        if pd.isna(value):
            return ""
        elif isinstance(value, pd.Timestamp):
            return value.strftime('%Y-%m-%d %H:%M')
        else:
            return str(value)
