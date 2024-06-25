import json
from datetime import datetime, date, timedelta
import logging
import pandas as pd


class WorkingDayCalculator:
    def __init__(self, calendar_df):
        self.calendars = {}
        for _, row in calendar_df.iterrows():
            calendar_id = str(row['clndr_id'])  # Ensure calendar_id is a string
            try:
                workdays = json.loads(row['parsed_workdays'])
                exceptions = json.loads(row['parsed_exceptions'])

                # Ensure workdays keys are strings
                workdays = {str(k): v for k, v in workdays.items()}

                # Convert exception dates to datetime.date objects
                exceptions = {self._parse_date(k): v for k, v in exceptions.items()}

                self.calendars[calendar_id] = {
                    'workdays': workdays,
                    'exceptions': exceptions
                }
            except json.JSONDecodeError as e:
                logging.warning(f"Failed to parse calendar data for {calendar_id}: {e}")

    def _parse_date(self, date_str):
        """Parse a date string to a datetime.date object."""
        try:
            return pd.to_datetime(date_str).date()
        except ValueError:
            logging.warning(f"Failed to parse date: {date_str}")
            return None

    def is_working_day(self, date_to_check, calendar_id):
        calendar_id = str(calendar_id)  # Ensure calendar_id is a string
        calendar = self.calendars.get(calendar_id)
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
        calendar_id = str(calendar_id)  # Ensure calendar_id is a string
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
        calendar_id = str(calendar_id)  # Ensure calendar_id is a string
        start_date = self._ensure_date(start_date)
        end_date = self._ensure_date(end_date)
        if start_date is None or end_date is None:
            return None

        if start_date > end_date:
            start_date, end_date = end_date, start_date

        working_days = 0
        current_date = start_date

        while current_date <= end_date:
            if self.is_working_day(current_date, calendar_id):
                working_days += 1
            current_date += timedelta(days=1)

        return working_days

    def get_working_hours_per_day(self, date_to_check, calendar_id):
        # This method could be implemented to handle partial working days
        # For now, it returns a default of 8 hours for a working day
        return 8 if self.is_working_day(date_to_check, calendar_id) else 0

    def _ensure_date(self, date_obj):
        """Ensure the input is a datetime.date object."""
        if isinstance(date_obj, datetime):
            return date_obj.date()
        elif isinstance(date_obj, date):
            return date_obj
        else:
            try:
                return pd.to_datetime(date_obj).date()
            except ValueError:
                logging.warning(f"Failed to convert to date: {date_obj}")
                return None