import logging

import pandas as pd
from xerparser import CODEC, Xer


class XerFileGenerator:
    def __init__(self, xer):
        self.xer = xer

    def create_modified_copy(self, progress_to_date: pd.Timestamp):
        """
        Create a copy of the entire Xer object with modified task progress and updated recalc date.

        Args:
            progress_to_date (datetime): The date to set as the new last_recalc_date and calculate progress against.

        Returns:
            Xer: A new Xer object with modified data.
        """
        new_xer = Xer.__new__(Xer)
        new_xer.tables = {key: df.copy() if isinstance(df, pd.DataFrame) else df for key, df in self.xer.tables.items()}
        new_xer.project_df = new_xer.tables.get('PROJECT', None)
        new_xer.task_df = new_xer.tables.get('TASK', None)
        new_xer.taskpred_df = new_xer.tables.get('TASKPRED', None)
        new_xer.projwbs_df = new_xer.tables.get('PROJWBS', None)
        new_xer.calendar_df = new_xer.tables.get('CALENDAR', None)
        new_xer.account_df = new_xer.tables.get('ACCOUNT', None)
        new_xer.workdays_df = self.xer.workdays_df.copy() if hasattr(self.xer, 'workdays_df') else pd.DataFrame()
        new_xer.exceptions_df = self.xer.exceptions_df.copy() if hasattr(self.xer, 'exceptions_df') else pd.DataFrame()

        # Update task progress
        if new_xer.task_df is not None:
            # Convert target_drtn_hr_cnt to numeric
            new_xer.task_df['target_drtn_hr_cnt'] = pd.to_numeric(new_xer.task_df['target_drtn_hr_cnt'],
                                                                  errors='coerce')

            new_xer.task_df['progress'] = new_xer.task_df.apply(
                lambda row: ProgressCalculator.calculate_progress(row, progress_to_date),
                axis=1
            )

            # Update remain_drtn_hr_cnt and target_drtn_hr_cnt
            new_xer.task_df['remain_drtn_hr_cnt'] = new_xer.task_df.apply(
                lambda row: ProgressCalculator.calculate_remaining_duration(row, progress_to_date),
                axis=1
            )

            # Handle tasks completed after progress_to_date
            completed_after_progress_mask = (new_xer.task_df['act_end_date'] > progress_to_date) & \
                                            (new_xer.task_df['act_end_date'].notnull())
            new_xer.task_df.loc[completed_after_progress_mask, 'act_end_date'] = pd.NaT

            # Reset actual dates for tasks with zero progress
            zero_progress_mask = new_xer.task_df['progress'] == 0
            new_xer.task_df.loc[zero_progress_mask, 'act_start_date'] = pd.NaT
            new_xer.task_df.loc[zero_progress_mask, 'act_end_date'] = pd.NaT
            new_xer.task_df.loc[zero_progress_mask, 'remain_drtn_hr_cnt'] = new_xer.task_df.loc[
                zero_progress_mask, 'target_drtn_hr_cnt']

            # Handle tasks that started after progress_to_date
            started_after_progress_mask = new_xer.task_df['act_start_date'] > progress_to_date
            new_xer.task_df.loc[started_after_progress_mask, 'act_start_date'] = pd.NaT
            new_xer.task_df.loc[started_after_progress_mask, 'act_end_date'] = pd.NaT
            new_xer.task_df.loc[started_after_progress_mask, 'remain_drtn_hr_cnt'] = new_xer.task_df.loc[
                started_after_progress_mask, 'target_drtn_hr_cnt']

        # Update last_recalc_date
        if new_xer.project_df is not None and 'last_recalc_date' in new_xer.project_df.columns:
            new_xer.project_df['last_recalc_date'] = progress_to_date.strftime('%Y-%m-%d %H:%M')

        return new_xer
    @staticmethod
    def generate_xer_contents(xer_obj: Xer) -> str:
        """Generate the XER file contents from the given Xer object."""
        xer_contents = ""

        # Handle ERMHDR specially
        if 'ERMHDR' in xer_obj.tables and not xer_obj.tables['ERMHDR'].empty:
            # If ERMHDR exists, use it and replace empty values with ''
            ermhdr_row = xer_obj.tables['ERMHDR'].iloc[0].fillna('')
            xer_contents += "ERMHDR\t" + "\t".join([str(x) for x in ermhdr_row]) + "\n"
        else:
            # If ERMHDR is missing or empty, create a minimal header based on the provided format
            xer_contents += "ERMHDR\t19.0\t2023-04-14\tProject\tUSER\tUSERNAME\tdbxDatabaseNoName\tProject Management\tUSD\n"

        for table_name, df in xer_obj.tables.items():
            if table_name != 'ERMHDR' and not df.empty:
                xer_contents += f"%T\t{table_name}\n"
                xer_contents += "%F\t" + "\t".join(df.columns) + "\n"
                for _, row in df.iterrows():
                    xer_contents += "%R\t" + "\t".join([XerFileGenerator._format_value(x) for x in row]) + "\n"

        return xer_contents

    @staticmethod
    def _format_value(value):
        """Format values for XER output, handling datetime objects."""
        if pd.isna(value):
            return ""
        elif isinstance(value, pd.Timestamp):
            return value.strftime('%Y-%m-%d %H:%M')
        else:
            return str(value)

    def generate_xer_file(self, output_file: str, date_prefix: str = None):
        if date_prefix:
            new_filename = f"{date_prefix}_{output_file}"
        else:
            new_filename = output_file
        self.build_xer_file(self.xer, new_filename)

    def build_xer_file(self, xer: Xer, output_file: str) -> str:
        """Generate and save the XER file."""
        # Ensure the output_file has a .xer extension
        if not output_file.lower().endswith('.xer'):
            output_file += '.xer'

        xer_contents = self.generate_xer_contents(xer)
        with open(output_file, 'w', encoding=CODEC) as f:
            f.write(xer_contents)
        logging.info(f"XER file exported to: {output_file}")
        return output_file


class ProgressCalculator:
    @staticmethod
    def calculate_progress(row, progress_to_date):
        if pd.isnull(row['act_start_date']):
            return 0.0
        elif pd.notnull(row['act_end_date']):
            if row['act_end_date'] <= progress_to_date:
                return 1.0
            else:
                # Task is completed now but wasn't at progress_to_date
                return ProgressCalculator.calculate_partial_progress(row, progress_to_date)
        elif row['act_start_date'] > progress_to_date:
            return 0.0
        else:
            return ProgressCalculator.calculate_partial_progress(row, progress_to_date)

    @staticmethod
    def calculate_partial_progress(row, progress_to_date):
        try:
            planned_duration = pd.Timedelta(hours=float(row['target_drtn_hr_cnt']))
            if planned_duration == pd.Timedelta(0):
                return 1.0  # If planned duration is 0, consider the task as complete
            actual_duration = progress_to_date - row['act_start_date']
            return min(actual_duration / planned_duration, 1.0)
        except (ValueError, TypeError):
            print(f"Warning: Invalid target_drtn_hr_cnt value for task {row['task_id']}: {row['target_drtn_hr_cnt']}")
            return 0.0

    @staticmethod
    def calculate_remaining_duration(row, progress_to_date):
        if pd.isnull(row['act_start_date']):
            return row['target_drtn_hr_cnt']
        elif pd.notnull(row['act_end_date']):
            if row['act_end_date'] <= progress_to_date:
                return 0
            else:
                # Task is completed now but wasn't at progress_to_date
                return ProgressCalculator.calculate_partial_remaining_duration(row, progress_to_date)
        else:
            return ProgressCalculator.calculate_partial_remaining_duration(row, progress_to_date)

    @staticmethod
    def calculate_partial_remaining_duration(row, progress_to_date):
        try:
            planned_duration = pd.Timedelta(hours=float(row['target_drtn_hr_cnt']))
            if planned_duration == pd.Timedelta(0):
                return 0  # If planned duration is 0, remaining duration is also 0
            actual_duration = progress_to_date - row['act_start_date']
            remaining_duration = max(planned_duration - actual_duration, pd.Timedelta(0))
            return remaining_duration.total_seconds() / 3600  # Convert to hours
        except (ValueError, TypeError):
            print(f"Warning: Invalid target_drtn_hr_cnt value for task {row['task_id']}: {row['target_drtn_hr_cnt']}")
            return row['target_drtn_hr_cnt']