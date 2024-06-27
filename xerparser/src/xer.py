from pathlib import Path
from typing import BinaryIO
from datetime import datetime, time, date
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
        self.workdays_df = pd.DataFrame(columns=['clndr_id', 'day', 'start_time', 'end_time'])
        self.exceptions_df = pd.DataFrame(columns=['clndr_id', 'exception_date', 'start_time', 'end_time'])

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

                # Add new DataFrames to xer_data
                self.workday_df = calendar_parser.workdays_df
                self.exception_df = calendar_parser.exceptions_df

            return xer_data
        else:
            raise ValueError("ValueError: invalid XER file")

    @staticmethod
    def _convert_times_to_strings(data):
        if isinstance(data, dict):
            return {k: Xer._convert_times_to_strings(v) for k, v in data.items() if k is not None}
        elif isinstance(data, list):
            return [Xer._convert_times_to_strings(item) for item in data]
        elif isinstance(data, tuple) and all(isinstance(x, time) for x in data):
            return [t.strftime('%H:%M') for t in data]
        elif isinstance(data, date):
            return data.isoformat()
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

    def create_modified_copy(self, split_date):
        """
        Create a copy of the entire Xer object with modified task progress and updated recalc date.

        Args:
            split_date (datetime): The date to set as the new last_recalc_date and calculate progress against.

        Returns:
            Xer: A new Xer object with modified data.
        """
        new_xer = Xer.__new__(Xer)
        new_xer.tables = {key: df.copy() if isinstance(df, pd.DataFrame) else df for key, df in self.tables.items()}
        new_xer.project_df = new_xer.tables.get('PROJECT', None)
        new_xer.task_df = new_xer.tables.get('TASK', None)
        new_xer.taskpred_df = new_xer.tables.get('TASKPRED', None)
        new_xer.projwbs_df = new_xer.tables.get('PROJWBS', None)
        new_xer.calendar_df = new_xer.tables.get('CALENDAR', None)
        new_xer.account_df = new_xer.tables.get('ACCOUNT', None)
        new_xer.workdays_df = self.workdays_df.copy() if hasattr(self, 'workdays_df') else pd.DataFrame()
        new_xer.exceptions_df = self.exceptions_df.copy() if hasattr(self, 'exceptions_df') else pd.DataFrame()

        # Update task progress
        if new_xer.task_df is not None:
            new_xer.task_df['progress'] = new_xer.task_df.apply(
                lambda row: self._calculate_progress(row, split_date),
                axis=1
            )

            # Reset actual dates for tasks with zero progress
            zero_progress_mask = new_xer.task_df['progress'] == 0
            new_xer.task_df.loc[zero_progress_mask, 'act_start_date'] = pd.NaT
            new_xer.task_df.loc[zero_progress_mask, 'act_end_date'] = pd.NaT

        # Update last_recalc_date
        if new_xer.project_df is not None and 'last_recalc_date' in new_xer.project_df.columns:
            new_xer.project_df['last_recalc_date'] = split_date.strftime('%Y-%m-%d %H:%M')

        return new_xer

    def _calculate_progress(self, row, split_date):
        if row['act_end_date'] <= split_date:
            return 1.0
        elif row['act_start_date'] > split_date:
            return 0.0
        elif pd.notnull(row['act_start_date']) and pd.notnull(row['act_end_date']):
            return (split_date - row['act_start_date']) / (row['act_end_date'] - row['act_start_date'])
        else:
            return 0.0

    def _format_value(self, value):
        """Format values for XER output, handling datetime objects."""
        if pd.isna(value):
            return ""
        elif isinstance(value, pd.Timestamp):
            return value.strftime('%Y-%m-%d %H:%M')
        else:
            return str(value)
