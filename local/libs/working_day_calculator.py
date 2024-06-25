# working_day_calculator.py

import json
from datetime import datetime, timedelta

class WorkingDayCalculator:
    def __init__(self, calendar_df):
        self.calendars = {}
        for _, row in calendar_df.iterrows():
            calendar_id = row['clndr_id']
            self.calendars[calendar_id] = {
                'workdays': json.loads(row['parsed_workdays']),
                'exceptions': {datetime.strptime(k, '%Y-%m-%d').date(): v for k, v in json.loads(row['parsed_exceptions']).items()}
            }

    def is_working_day(self, date, calendar_id):
        calendar = self.calendars.get(calendar_id)
        if not calendar:
            return False

        if date in calendar['exceptions']:
            return bool(calendar['exceptions'][date])

        return bool(calendar['workdays'].get(str(date.weekday() + 1)))

    def add_working_days(self, start_date, days, calendar_id):
        current_date = start_date
        remaining_days = days

        while remaining_days > 0:
            current_date += timedelta(days=1)
            if self.is_working_day(current_date, calendar_id):
                remaining_days -= 1

        return current_date

    def get_working_days_between(self, start_date, end_date, calendar_id):
        working_days = 0
        current_date = start_date

        while current_date <= end_date:
            if self.is_working_day(current_date, calendar_id):
                working_days += 1
            current_date += timedelta(days=1)

        return working_days