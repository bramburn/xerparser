import re
from datetime import datetime, timedelta
import pandas as pd

class CalendarParser:
    def __init__(self, calendar_df):
        self.calendar_df = calendar_df
        self.calendars = {}

    def parse_calendars(self):
        for _, row in self.calendar_df.iterrows():
            self.parse_calendar_data(row['clndr_id'], row['clndr_data'])

    def parse_calendar_data(self, calendar_id, clndr_data):
        workdays = self._parse_workdays(clndr_data)
        exceptions = self._parse_exceptions(clndr_data)
        self.calendars[calendar_id] = {
            'workdays': workdays,
            'exceptions': exceptions
        }

    def _parse_workdays(self, clndr_data):
        workday_pattern = r'\(0\|\|([1-7])\(\)([^()]*)\)'
        workdays = {}
        for match in re.finditer(workday_pattern, clndr_data):
            day = int(match.group(1))
            hours = self._parse_hours(match.group(2))
            workdays[day] = hours
        return workdays

    def _parse_exceptions(self, clndr_data):
        exception_pattern = r'\(d\|(\d+)\)\(([^()]*)\)'
        exceptions = {}
        for match in re.finditer(exception_pattern, clndr_data):
            date = self._excel_date_to_datetime(int(match.group(1)))
            hours = self._parse_hours(match.group(2))
            exceptions[date] = hours
        return exceptions

    def _parse_hours(self, hours_str):
        hour_pattern = r's\|(\d{2}:\d{2})\|f\|(\d{2}:\d{2})'
        return [tuple(map(self._parse_time, match.groups())) for match in re.finditer(hour_pattern, hours_str)]

    @staticmethod
    def _parse_time(time_str):
        return datetime.strptime(time_str, '%H:%M').time()

    @staticmethod
    def _excel_date_to_datetime(excel_date):
        return datetime(1899, 12, 30) + timedelta(days=excel_date)

    def is_working_day(self, calendar_id, date):
        calendar = self.calendars.get(calendar_id)
        if not calendar:
            return False

        if date in calendar['exceptions']:
            return bool(calendar['exceptions'][date])

        return bool(calendar['workdays'].get(date.weekday() + 1))

    def get_working_hours(self, calendar_id, date):
        calendar = self.calendars.get(calendar_id)
        if not calendar:
            return []

        if date in calendar['exceptions']:
            return calendar['exceptions'][date]

        return calendar['workdays'].get(date.weekday() + 1, [])

    def add_working_days(self, calendar_id, start_date, days):
        current_date = start_date
        remaining_days = days

        while remaining_days > 0:
            current_date += timedelta(days=1)
            if self.is_working_day(calendar_id, current_date):
                remaining_days -= 1

        return current_date

    def get_working_days_between(self, calendar_id, start_date, end_date):
        working_days = 0
        current_date = start_date

        while current_date <= end_date:
            if self.is_working_day(calendar_id, current_date):
                working_days += 1
            current_date += timedelta(days=1)

        return working_days