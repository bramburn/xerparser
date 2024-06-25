import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta
import logging


class WorkingDayCalculator:
    def __init__(self, workdays_df, exceptions_df):
        self.workdays_df = workdays_df.copy() if not workdays_df.empty else pd.DataFrame()
        self.exceptions_df = exceptions_df.copy() if not exceptions_df.empty else pd.DataFrame()

        # Create a dictionary for faster lookups
        self.calendar_dict = self._create_calendar_dict()

    def _create_calendar_dict(self):
        calendar_dict = {}

        # Process workdays
        for _, row in self.workdays_df.iterrows():
            calendar_id = str(row['clndr_id'])
            day = int(row['day'])  # Assuming 'day' is an integer representing the day of the week
            if calendar_id not in calendar_dict:
                calendar_dict[calendar_id] = {'workdays': {}, 'exceptions': {}}

            if day not in calendar_dict[calendar_id]['workdays']:
                calendar_dict[calendar_id]['workdays'][day] = []

            if pd.notna(row['start_time']) and pd.notna(row['end_time']):
                calendar_dict[calendar_id]['workdays'][day].append((row['start_time'], row['end_time']))

        # Process exceptions
        for _, row in self.exceptions_df.iterrows():
            calendar_id = str(row['clndr_id'])
            exception_date = row['exception_date'].date()
            if calendar_id not in calendar_dict:
                calendar_dict[calendar_id] = {'workdays': {}, 'exceptions': {}}

            if exception_date not in calendar_dict[calendar_id]['exceptions']:
                calendar_dict[calendar_id]['exceptions'][exception_date] = []

            if pd.notna(row['start_time']) and pd.notna(row['end_time']):
                calendar_dict[calendar_id]['exceptions'][exception_date].append((row['start_time'], row['end_time']))

        return calendar_dict

    def is_working_day(self, date_to_check, calendar_id):
        calendar_id = str(calendar_id)
        calendar = self.calendar_dict.get(calendar_id)
        if not calendar:
            logging.warning(f"Calendar {calendar_id} not found")
            return False

        date_to_check = self._ensure_date(date_to_check)
        if date_to_check is None:
            return False

        if date_to_check in calendar['exceptions']:
            return bool(calendar['exceptions'][date_to_check])

        return bool(calendar['workdays'].get(str(date_to_check.weekday() + 1)))

    def add_working_days(self, start_date, days, calendar_id):
        calendar_id = str(calendar_id)
        start_date = self._ensure_date(start_date)
        if start_date is None:
            return None

        try:
            days = int(days)
        except ValueError:
            logging.warning(f"Invalid days value: {days}")
            return None

        current_date = start_date
        remaining_days = abs(days)
        day_increment = 1 if days >= 0 else -1

        while remaining_days > 0:
            current_date += timedelta(days=day_increment)
            if self.is_working_day(current_date, calendar_id):
                remaining_days -= 1

        return current_date

    def get_working_days_between(self, start_date, end_date, calendar_id):
        calendar_id = str(calendar_id)
        start_date = self._ensure_date(start_date)
        end_date = self._ensure_date(end_date)
        if start_date is None or end_date is None:
            return None

        if start_date > end_date:
            start_date, end_date = end_date, start_date

        date_range = pd.date_range(start=start_date, end=end_date)
        working_days = sum(self.is_working_day(date, calendar_id) for date in date_range)

        return working_days

    def get_working_hours_per_day(self, date_to_check, calendar_id):
        calendar_id = str(calendar_id)
        calendar = self.calendar_dict.get(calendar_id)
        if not calendar:
            logging.warning(f"Calendar {calendar_id} not found")
            return 0

        date_to_check = self._ensure_date(date_to_check)
        if date_to_check is None:
            return 0

        if date_to_check in calendar['exceptions']:
            time_tuples = calendar['exceptions'][date_to_check]
        else:
            time_tuples = calendar['workdays'].get(str(date_to_check.weekday() + 1), [])

        total_hours = sum((end.hour - start.hour + (end.minute - start.minute) / 60)
                          for start, end in time_tuples if start and end)
        return total_hours

    def _ensure_date(self, date_obj):
        if isinstance(date_obj, (datetime, date)):
            return date_obj.date() if isinstance(date_obj, datetime) else date_obj
        try:
            return pd.to_datetime(date_obj).date()
        except ValueError:
            logging.warning(f"Failed to convert to date: {date_obj}")
            return None
