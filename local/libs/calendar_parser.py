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

            # Validate parsed dates
            valid_exceptions = {k: v for k, v in exceptions.items() if isinstance(k, date)}
            if len(valid_exceptions) != len(exceptions):
                self.logger.warning(f"Some exception dates were invalid for calendar {calendar_id}")

            self.calendars[calendar_id] = {
                'workdays': workdays,
                'exceptions': valid_exceptions
            }
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
            excel_date = float(excel_date_str)  # Use float instead of int to handle fractional days
        except ValueError:
            logging.warning(f"Invalid Excel date format: {excel_date_str}")
            return None

        # Handle dates outside of Python's datetime range
        if excel_date < -693593 or excel_date > 2958465:
            logging.warning(f"Excel date {excel_date} is outside the valid range for Python's datetime.")
            return None

        try:
            # Excel's date system has two different starting dates
            if excel_date >= 60:
                # For dates after February 28, 1900
                base_date = date(1899, 12, 30)
            else:
                # For dates before March 1, 1900
                base_date = date(1900, 1, 1)

            delta = timedelta(days=int(excel_date) - 1)  # Subtract 1 because Excel considers January 1, 1900 as day 1
            return base_date + delta
        except (ValueError, OverflowError) as e:
            logging.warning(f"Error converting Excel date {excel_date_str}: {str(e)}")
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

    def is_working_day(self, calendar_id: Union[str, int], date: Union[str, datetime]) -> bool:
        calendar = self.calendars.get(str(calendar_id))
        if not calendar:
            self.logger.warning(f"Calendar not found: {calendar_id}")
            return False

        if not isinstance(date, datetime):
            try:
                date = pd.to_datetime(date)
            except ValueError:
                self.logger.error(f"Invalid date format: {date}")
                return False

        if date.date() in calendar['exceptions']:
            return bool(calendar['exceptions'][date.date()])

        return bool(calendar['workdays'].get(date.weekday() + 1))

    def get_working_hours(self, calendar_id: Union[str, int], date: Union[str, datetime]) -> List[Tuple[time, time]]:
        calendar = self.calendars.get(str(calendar_id))
        if not calendar:
            self.logger.warning(f"Calendar not found: {calendar_id}")
            return []

        if not isinstance(date, datetime):
            try:
                date = pd.to_datetime(date)
            except ValueError:
                self.logger.error(f"Invalid date format: {date}")
                return []

        if date.date() in calendar['exceptions']:
            return calendar['exceptions'][date.date()]

        return calendar['workdays'].get(date.weekday() + 1, [])

    def add_working_days(self, calendar_id: Union[str, int], start_date: Union[str, datetime],
                         days: Union[str, int]) -> datetime:
        if not isinstance(start_date, datetime):
            try:
                start_date = pd.to_datetime(start_date)
            except ValueError:
                raise ValueError(f"Invalid start_date format: {start_date}")

        try:
            days = int(days)
        except ValueError:
            raise ValueError(f"Invalid days value: {days}. Must be convertible to an integer.")

        current_date: datetime = start_date
        remaining_days: int = days

        while remaining_days > 0:
            current_date += timedelta(days=1)
            if self.is_working_day(calendar_id, current_date):
                remaining_days -= 1

        return current_date

    def get_working_days_between(self, calendar_id: Union[str, int], start_date: Union[str, datetime],
                                 end_date: Union[str, datetime]) -> int:
        if not isinstance(start_date, datetime):
            try:
                start_date = pd.to_datetime(start_date)
            except ValueError:
                raise ValueError(f"Invalid start_date format: {start_date}")

        if not isinstance(end_date, datetime):
            try:
                end_date = pd.to_datetime(end_date)
            except ValueError:
                raise ValueError(f"Invalid end_date format: {end_date}")

        if start_date > end_date:
            raise ValueError("start_date must be before or equal to end_date")

        working_days: int = 0
        current_date: datetime = start_date

        while current_date <= end_date:
            if self.is_working_day(calendar_id, current_date):
                working_days += 1
            current_date += timedelta(days=1)

        return working_days
