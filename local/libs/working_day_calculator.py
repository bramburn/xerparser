import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta
import logging


class WorkingDayCalculator:
    def __init__(self, workdays_df, exceptions_df):
        self.workdays_df = workdays_df.copy() if not workdays_df.empty else pd.DataFrame()
        self.exceptions_df = exceptions_df.copy() if not exceptions_df.empty else pd.DataFrame()

    def is_working_day(self, date_to_check, calendar_id):
        calendar_id = str(calendar_id)

        date_to_check = self._ensure_date(date_to_check)
        if date_to_check is None:
            return False

        # Check exceptions first
        if 'clndr_id' in self.exceptions_df.columns and 'exception_date' in self.exceptions_df.columns:
            exception_rows = self.exceptions_df[
                (self.exceptions_df['clndr_id'] == calendar_id) &
                (self.exceptions_df['exception_date'] == date_to_check)
                ]
            if not exception_rows.empty:
                # Check if there are any working hours defined for this exception date
                has_working_hours = exception_rows[
                                        (exception_rows['start_time'].notna()) &
                                        (exception_rows['end_time'].notna())
                                        ].shape[0] > 0
                return has_working_hours

        # If not an exception, check regular workdays
        if 'clndr_id' in self.workdays_df.columns and 'day' in self.workdays_df.columns:
            weekday = date_to_check.isoweekday()  # Get weekday as 1-7
            workday_rows = self.workdays_df[
                (self.workdays_df['clndr_id'] == calendar_id) &
                (self.workdays_df['day'] == weekday)
                ]
            if not workday_rows.empty:
                # Check if there are any working hours defined for this day
                has_working_hours = workday_rows[
                                        (workday_rows['start_time'].notna()) &
                                        (workday_rows['end_time'].notna())
                                        ].shape[0] > 0
                return has_working_hours

        return False

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

    def _ensure_date(self, date_obj):
        if isinstance(date_obj, (datetime, date)):
            return date_obj.date() if isinstance(date_obj, datetime) else date_obj
        try:
            return pd.to_datetime(date_obj).date()
        except ValueError:
            logging.warning(f"Failed to convert to date: {date_obj}")
            return None
