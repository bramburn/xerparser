import re
from datetime import datetime, timedelta, time, date
import pandas as pd
import logging
from typing import Dict, List, Tuple, Union


class CalendarParser:
    def __init__(self, calendar_df: pd.DataFrame) -> None:
        if not isinstance(calendar_df, pd.DataFrame):
            raise ValueError("calendar_df must be a pandas DataFrame")

        required_columns = ['clndr_id', 'clndr_data']
        missing_columns = [col for col in required_columns if col not in calendar_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

        self.calendar_df: pd.DataFrame = calendar_df
        self.calendars: Dict[
            str, Dict[str, Union[Dict[int, List[Tuple[time, time]]], Dict[date, List[Tuple[time, time]]]]]] = {}
        self.workdays_df = pd.DataFrame(columns=['clndr_id', 'day', 'start_time', 'end_time'])
        self.exceptions_df = pd.DataFrame(columns=['clndr_id', 'exception_date', 'start_time', 'end_time'])
        self.logger = logging.getLogger(__name__)

    def parse_calendars(self) -> None:
        for _, row in self.calendar_df.iterrows():
            try:
                self.parse_calendar_data(str(row['clndr_id']), str(row['clndr_data']))
            except Exception as e:
                self.logger.error(f"Error parsing calendar {row['clndr_id']}: {str(e)}")

    def parse_calendar_data(self, calendar_id: str, clndr_data: str) -> None:
        if not calendar_id or not clndr_data:
            self.logger.warning(f"Skipping calendar with empty id or data: {calendar_id}")
            return

        try:
            workdays = self._parse_workdays(clndr_data)
            exceptions = self._parse_exceptions(clndr_data)

            for day, hours in workdays.items():
                for start, end in hours:
                    self.workdays_df = pd.concat([self.workdays_df, pd.DataFrame({
                        'clndr_id': [calendar_id],
                        'day': [day],
                        'start_time': [start],
                        'end_time': [end]
                    })], ignore_index=True)

            for exception_date, hours in exceptions.items():
                if hours:
                    for start, end in hours:
                        self.exceptions_df = pd.concat([self.exceptions_df, pd.DataFrame({
                            'clndr_id': [calendar_id],
                            'exception_date': [exception_date],
                            'start_time': [start],
                            'end_time': [end]
                        })], ignore_index=True)
                else:
                    self.exceptions_df = pd.concat([self.exceptions_df, pd.DataFrame({
                        'clndr_id': [calendar_id],
                        'exception_date': [exception_date],
                        'start_time': [None],
                        'end_time': [None]
                    })], ignore_index=True)

        except Exception as e:
            self.logger.error(f"Error parsing calendar data for {calendar_id}: {str(e)}")

    def _parse_workdays(self, clndr_data: str) -> Dict[int, List[Tuple[time, time]]]:
        workday_pattern = r'\(0\|\|([1-7])\(\)([^()]*)\)'
        workdays: Dict[int, List[Tuple[time, time]]] = {}
        for match in re.finditer(workday_pattern, clndr_data):
            try:
                day = int(match.group(1))
                hours_str = match.group(2)
                if hours_str.strip():
                    hours = self._parse_hours(hours_str)
                    workdays[day] = self._merge_overlapping_hours(hours)
                else:
                    workdays[day] = []  # Empty list for non-working days
            except ValueError as e:
                self.logger.warning(f"Invalid workday data: {match.group()}. Error: {str(e)}")
        return workdays

    @staticmethod
    def _excel_date_to_datetime(excel_date_str: str) -> Union[date, None]:
        try:
            # First, try to convert the string to a pandas datetime object
            python_date = pd.to_datetime(excel_date_str, errors='coerce')

            # If it's a valid datetime, convert it to a date object
            if pd.notnull(python_date):
                return python_date.date()

        except ValueError:
            pass  # If conversion fails, proceed to the next check

        try:
            # If the previous conversion failed, try to convert to float (for Excel dates)
            excel_date = float(excel_date_str)
            python_date = pd.to_datetime('1899-12-30') + pd.to_timedelta(excel_date, 'D')
            return python_date.date()
        except ValueError:
            logging.warning(f"Invalid Excel date format: {excel_date_str}")
            return None
    def _parse_exceptions(self, clndr_data: str) -> Dict[date, List[Tuple[time, time]]]:
        exception_pattern = r'\(0\|\|(\d+)\(d\|(\d+)\)(?:\((.*?)\))?\(\)\)'
        exceptions: Dict[date, List[Tuple[time, time]]] = {}
        for match in re.finditer(exception_pattern, clndr_data):
            try:
                exception_date = self._excel_date_to_datetime(match.group(2))
                if exception_date is None:
                    self.logger.warning(f"Skipping exception date: {match.group(2)}")
                    continue  # Skip this exception if the date couldn't be parsed
                hours_str = match.group(3) if match.group(3) else ""
                if hours_str.strip():
                    hours = self._parse_hours(hours_str)
                    exceptions[exception_date] = self._merge_overlapping_hours(hours)
                else:
                    exceptions[exception_date] = []  # Empty list for non-working exception days
            except ValueError as e:
                self.logger.warning(f"Invalid exception data: {match.group()}. Error: {str(e)}")
        return exceptions
    def _parse_hours(self, hours_str: str) -> List[Tuple[time, time]]:
        hour_pattern = r's\|(\d{2}:\d{2})\|f\|(\d{2}:\d{2})'
        hours: List[Tuple[time, time]] = []
        for match in re.finditer(hour_pattern, hours_str):
            try:
                start, end = map(self._parse_time, match.groups())
                hours.append((start, end))
            except ValueError as e:
                self.logger.warning(f"Invalid hour format: {match.group()}. Error: {str(e)}")
        return hours

    @staticmethod
    def _parse_time(time_str: str) -> time:
        try:
            return datetime.strptime(time_str, '%H:%M').time()
        except ValueError:
            raise ValueError(f"Invalid time format: {time_str}")



    @staticmethod
    def _merge_overlapping_hours(hours: List[Tuple[time, time]]) -> List[Tuple[time, time]]:
        if not hours:
            return []
        sorted_hours = sorted(hours, key=lambda x: x[0])
        merged: List[Tuple[time, time]] = [sorted_hours[0]]
        for current in sorted_hours[1:]:
            previous = merged[-1]
            if current[0] <= previous[1]:
                merged[-1] = (previous[0], max(previous[1], current[1]))
            else:
                merged.append(current)
        return merged





