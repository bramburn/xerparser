import json
from datetime import datetime, date, timedelta
import logging

class WorkingDayCalculator:
    def __init__(self, calendar_df):
        self.calendars = {}
        for _, row in calendar_df.iterrows():
            calendar_id = row['clndr_id']
            try:
                self.calendars[calendar_id] = {
                    'workdays': json.loads(row['parsed_workdays']),
                    'exceptions': {datetime.strptime(k, '%Y-%m-%d').date(): v 
                                   for k, v in json.loads(row['parsed_exceptions']).items()}
                }
            except json.JSONDecodeError as e:
                logging.warning(f"Failed to parse calendar data for {calendar_id}: {e}")

    def is_working_day(self, date_to_check, calendar_id):
        calendar = self.calendars.get(calendar_id)
        if not calendar:
            logging.warning(f"Calendar {calendar_id} not found")
            return False

        if isinstance(date_to_check, datetime):
            date_to_check = date_to_check.date()

        if date_to_check in calendar['exceptions']:
            return bool(calendar['exceptions'][date_to_check])

        return bool(calendar['workdays'].get(str(date_to_check.weekday() + 1)))

    def add_working_days(self, start_date, days, calendar_id):
        if isinstance(start_date, datetime):
            start_date = start_date.date()

        current_date = start_date
        remaining_days = abs(days)
        day_increment = 1 if days >= 0 else -1

        while remaining_days > 0:
            current_date += timedelta(days=day_increment)
            if self.is_working_day(current_date, calendar_id):
                remaining_days -= 1

        return current_date

    def get_working_days_between(self, start_date, end_date, calendar_id):
        if isinstance(start_date, datetime):
            start_date = start_date.date()
        if isinstance(end_date, datetime):
            end_date = end_date.date()

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