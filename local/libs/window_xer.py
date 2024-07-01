import logging
import os
import datetime

from typing import Tuple, NamedTuple, Union, Optional, List, Dict, Set

import pandas as pd
from mdutils import MdUtils

from local.libs.total_float_method import TotalFloatCPMCalculator
from local.libs.xer_file_creation import XerFileGenerator
from xerparser import Xer


class WindowXER(NamedTuple):
    xer: Xer
    critical_path: list
    file_path: str


class WindowAnalyzer:
    def __init__(self, xer: Xer, start_window_folder_path: str, end_window_folder_path: str):
        """
        Initializes a new instance of the WindowAnalyzer class.

        Args:
            xer (Xer): The Xer object representing the original XER file.
            start_window_folder_path (str): The path to the folder where the start window XER files will be saved.
            end_window_folder_path (str): The path to the folder where the end window XER files will be saved.

        Returns:
            None
        """
        self.xer = xer
        self.start_window_xer_folder_path = start_window_folder_path
        self.end_window_xer_folder_path = end_window_folder_path
        self.xer_generator = XerFileGenerator(self.xer)

    def process_window(self, date: pd.Timestamp, is_end_window: bool) -> WindowXER:
        """
        Process the window based on the given date and window type.

        Parameters:
            date (pd.Timestamp): The date for which the window is being processed.
            is_end_window (bool): A boolean indicating whether it's an end window or not.

        Returns:
            WindowXER: A named tuple containing the processed window XER, critical path information, and file path.
        """
        window_xer = self.xer_generator.create_modified_copy(date)

        calculator = TotalFloatCPMCalculator(window_xer)
        calculator.set_workdays_df(window_xer.workdays_df)
        calculator.set_exceptions_df(window_xer.exceptions_df)
        critical_path = calculator.calculate_critical_path()
        calculator.update_task_df()

        folder_path = self.end_window_xer_folder_path if is_end_window else self.start_window_xer_folder_path
        window_type = "end" if is_end_window else "start"
        file_name = os.path.join(folder_path, f"{date.strftime('%Y-%m-%d')}_{window_type}_window.xer")
        self.xer_generator.build_xer_file(window_xer, file_name)

        return WindowXER(window_xer, critical_path, file_name)

    def filter_tasks(self, tasks_df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        # Convert target_start_date and target_end_date to datetime if they are not
        tasks_df['target_start_date'] = pd.to_datetime(tasks_df['target_start_date'])
        tasks_df['target_end_date'] = pd.to_datetime(tasks_df['target_end_date'])

        return tasks_df[
            (tasks_df['target_start_date'] >= start_date) &
            (tasks_df['target_end_date'] <= end_date) &
            (tasks_df['task_type'].isin(['TT_Mile', 'TT_FinMile', 'TT_Task']))
            ]

    def add_table_of_contents(self, mdFile: MdUtils):
        mdFile.new_header(level=1, title="Table of Contents")
        mdFile.new_table_of_contents(table_title="Contents", depth=2)

    def generate_markdown_report(self, start_window: WindowXER, end_window: WindowXER, start_date: pd.Timestamp,
                                 end_date: pd.Timestamp):
        mdFile = MdUtils(
            file_name=f"window_analysis_report_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}",
            title="Window Analysis Report")

        self.add_table_of_contents(mdFile)
        self.add_window_period(mdFile, start_date, end_date)
        self.add_critical_path_comparison(mdFile, start_window, end_window)
        self.add_activities_in_period(mdFile, start_window, end_window, start_date, end_date)
        # Add this new line
        self.generate_rapid_completion_report(mdFile, end_window)

        mdFile.create_md_file()
        print(f"Markdown report saved as: {mdFile.file_name}.md")

    def add_window_period(self, mdFile: MdUtils, start_date: pd.Timestamp, end_date: pd.Timestamp):
        mdFile.new_header(level=1, title="Window Period")
        mdFile.new_paragraph(f"Start Date: {start_date.strftime('%Y-%m-%d')}")
        mdFile.new_paragraph(f"End Date: {end_date.strftime('%Y-%m-%d')}")

    def format_date(self, date: Union[pd.Timestamp, datetime.date, str, None], suffix: str = '') -> Optional[str]:
        if pd.notnull(date):
            try:
                formatted_date = pd.to_datetime(date).strftime('%Y-%m-%d')
                return f"{formatted_date}{suffix}"
            except ValueError:
                return str(date)  # Return the original string if it can't be parsed
        return None

    def add_critical_path_comparison(self, md_file_utils: MdUtils, start_window: WindowXER, end_window: WindowXER):
        md_file_utils.new_header(level=1, title="Critical Path Comparison")

        start_critical_path = start_window.critical_path
        end_critical_path = end_window.critical_path

        # Convert last_recalc_date to Timestamp
        start_window_date = pd.to_datetime(start_window.xer.project_df['last_recalc_date'].iloc[0])

        # Find the index where the critical paths start to differ
        divergence_index = 0
        for i, (start_task, end_task) in enumerate(zip(start_critical_path, end_critical_path)):
            if start_task != end_task:
                divergence_index = max(0, i - 1)  # Include one task before the change
                break

        # Create sets for efficient lookup
        start_critical_set = set(start_critical_path[divergence_index:])
        end_critical_set = set(end_critical_path[divergence_index:])
        all_critical_tasks = start_critical_set.union(end_critical_set)

        table_headers = ["Task Code", "Task Name", "Start", "End", "Critical 1*", "Critical 2**"]
        table_data = table_headers.copy()

        for task_id in all_critical_tasks:
            start_task = start_window.xer.task_df[start_window.xer.task_df['task_id'] == task_id].iloc[
                0] if task_id in start_critical_set else None
            end_task = end_window.xer.task_df[end_window.xer.task_df['task_id'] == task_id].iloc[
                0] if task_id in end_critical_set else None

            task: pd.Series = end_task if end_task is not None else start_task

            # Check if the task is older than start_window
            task_start_date = pd.to_datetime(task['act_start_date'] or task['target_start_date'])
            if task_start_date >= start_window_date:
                continue

            task_code = task['task_code']
            task_name = task['task_name']

            start_date = self.format_date(task['act_start_date'], 'A') or self.format_date(
                task['target_start_date']) or "N/A"
            end_date = self.format_date(task['act_end_date'], 'A') or self.format_date(task['target_end_date']) or "N/A"

            critical_1 = "True" if task_id in start_critical_set else "False"
            critical_2 = "True" if task_id in end_critical_set else "False"

            table_data.extend([task_code, task_name, start_date, end_date, critical_1, critical_2])

        num_rows = len(table_data) // 6  # 6 is the number of columns
        md_file_utils.new_table(columns=6, rows=num_rows, text=table_data, text_align='left')

        md_file_utils.new_paragraph("*Critical 1: Critical path status at the start of the window")
        md_file_utils.new_paragraph("**Critical 2: Critical path status at the end of the window")
        md_file_utils.new_paragraph("A: Actual date")

        # Summary of changes
        removed_tasks = start_critical_set - end_critical_set
        added_tasks = end_critical_set - start_critical_set

        md_file_utils.new_header(level=2, title="Summary of Critical Path Changes")

        summary_headers = ["Change Type", "Task Code", "Task Name"]
        summary_data = summary_headers.copy()

        for task_id in removed_tasks:
            task = start_window.xer.task_df[start_window.xer.task_df['task_id'] == task_id].iloc[0]
            task_start_date = pd.to_datetime(task['act_start_date'] or task['target_start_date'])
            if task_start_date < start_window_date:
                summary_data.extend(["Removed", task['task_code'], task['task_name']])

        for task_id in added_tasks:
            task = end_window.xer.task_df[end_window.xer.task_df['task_id'] == task_id].iloc[0]
            task_start_date = pd.to_datetime(task['act_start_date'] or task['target_start_date'])
            if task_start_date < start_window_date:
                summary_data.extend(["Added", task['task_code'], task['task_name']])

        num_rows = len(summary_data) // 3  # 3 is the number of columns
        md_file_utils.new_table(columns=3, rows=num_rows, text=summary_data, text_align='left')

        # New critical path
        md_file_utils.new_header(level=2, title="New Critical Path")
        new_critical_path = [task for task in end_critical_path[divergence_index:]
                             if pd.to_datetime(
                end_window.xer.task_df[end_window.xer.task_df['task_id'] == task].iloc[0]['act_start_date'] or
                end_window.xer.task_df[end_window.xer.task_df['task_id'] == task].iloc[0]['target_start_date'])
                             < start_window_date]
        new_critical_path_text = " -> ".join(
            [end_window.xer.task_df[end_window.xer.task_df['task_id'] == task_id].iloc[0]['task_code'] for task_id in
             new_critical_path])
        md_file_utils.new_paragraph(
            f"New critical path from the point of change (only tasks older than start window): {new_critical_path_text}")

    def add_activities_in_period(self, mdFile: MdUtils, start_window: WindowXER, end_window: WindowXER,
                                 start_date: pd.Timestamp, end_date: pd.Timestamp):

        mdFile.new_header(level=1, title="Activities in the Period")

        # Original table (unchanged)
        start_tasks = self.filter_tasks(start_window.xer.task_df, start_date, end_date)
        end_tasks = self.filter_tasks(end_window.xer.task_df, start_date, end_date)

        all_task_codes = set(start_tasks['task_code']).union(set(end_tasks['task_code']))

        activities_table = ["Task Code", "Task Name", "Planned Start", "Planned End", "Planned Duration",
                            "Actual Start", "Actual End", "Actual Duration"]

        for task_code in all_task_codes:
            row = self.get_activity_row(task_code, start_tasks, end_tasks)
            activities_table.extend(row)

        mdFile.new_table(columns=8, rows=len(all_task_codes) + 1, text=activities_table, text_align='left')

        # New table: Activities Planned in the Period
        mdFile.new_header(level=2, title="Activities Planned in the Period")
        planned_activities = end_window.xer.task_df[
            (end_window.xer.task_df['target_start_date'] >= start_date) &
            (end_window.xer.task_df['target_start_date'] <= end_date)
            ]

        planned_table = ["Task Code", "Task Name", "Planned Start", "Planned End"]
        for _, task in planned_activities.iterrows():
            planned_table.extend([
                task['task_code'],
                task['task_name'],
                self.format_date(task['target_start_date']) or "N/A",
                self.format_date(task['target_end_date']) or "N/A"
            ])

        mdFile.new_table(columns=4, rows=len(planned_activities) + 1, text=planned_table, text_align='left')

        # New table: Activities Completed in the Period
        mdFile.new_header(level=2, title="Activities Completed in the Period")
        completed_activities = end_window.xer.task_df[
            (end_window.xer.task_df['act_end_date'] >= start_date) &
            (end_window.xer.task_df['act_end_date'] <= end_date)
            ]

        completed_table = ["Task Code", "Task Name", "Actual Start", "Actual End", "Planned Duration",
                           "Actual Duration"]
        for _, task in completed_activities.iterrows():
            planned_duration = (
                        pd.to_datetime(task['target_end_date']) - pd.to_datetime(task['target_start_date'])).days
            actual_duration = (pd.to_datetime(task['act_end_date']) - pd.to_datetime(task['act_start_date'])).days

            completed_table.extend([
                task['task_code'],
                task['task_name'],
                self.format_date(task['act_start_date']) or "N/A",
                self.format_date(task['act_end_date']) or "N/A",
                f"{planned_duration} days",
                f"{actual_duration} days"
            ])

        mdFile.new_table(columns=6, rows=len(completed_activities) + 1, text=completed_table, text_align='left')

        # New table: Activities Started in the Period
        mdFile.new_header(level=2, title="Activities Started in the Period")
        started_activities = end_window.xer.task_df[
            (end_window.xer.task_df['act_start_date'] >= start_date) &
            (end_window.xer.task_df['act_start_date'] <= end_date)
            ]

        started_table = ["Task Code", "Task Name", "Actual Start", "Planned End"]
        for _, task in started_activities.iterrows():
            started_table.extend([
                task['task_code'],
                task['task_name'],
                self.format_date(task['act_start_date']) or "N/A",
                self.format_date(task['target_end_date']) or "N/A"
            ])

        mdFile.new_table(columns=4, rows=len(started_activities) + 1, text=started_table, text_align='left')

    def generate_rapid_completion_report(self, mdFile: MdUtils, end_window: WindowXER):
        mdFile.new_header(level=1, title="Rapidly Completed Activities Report")

        # Filter completed activities
        completed_activities = end_window.xer.task_df[
            (pd.notnull(end_window.xer.task_df['act_start_date'])) &
            (pd.notnull(end_window.xer.task_df['act_end_date']))
            ].copy()  # Create a copy to avoid SettingWithCopyWarning

        # Calculate planned and actual durations
        completed_activities['planned_duration'] = (
                pd.to_datetime(completed_activities['target_end_date']) -
                pd.to_datetime(completed_activities['target_start_date'])
        ).dt.days
        completed_activities['actual_duration'] = (
                pd.to_datetime(completed_activities['act_end_date']) -
                pd.to_datetime(completed_activities['act_start_date'])
        ).dt.days

        # Filter activities with planned duration >= 1 day and completed in 70% or less time
        rapid_activities = completed_activities[
            (completed_activities['planned_duration'] >= 1) &
            (completed_activities['actual_duration'] <= 0.7 * completed_activities['planned_duration'])
            ].copy()  # Create another copy for the filtered dataset

        # Calculate completion percentage
        rapid_activities['completion_percentage'] = (
                rapid_activities['actual_duration'] / rapid_activities['planned_duration'] * 100
        ).round(2)

        # Sort by completion percentage
        rapid_activities = rapid_activities.sort_values('completion_percentage')

        # Create table
        table_headers = ["Task Code", "Task Name", "Planned Duration (days)", "Actual Duration (days)",
                         "Completion Percentage"]
        table_data = table_headers.copy()

        for _, task in rapid_activities.iterrows():
            table_data.extend([
                task['task_code'],
                task['task_name'],
                str(task['planned_duration']),
                str(task['actual_duration']),
                f"{task['completion_percentage']}%"
            ])

        mdFile.new_table(columns=5, rows=len(rapid_activities) + 1, text=table_data, text_align='left')

        # Add summary
        mdFile.new_paragraph(f"Total rapidly completed activities: {len(rapid_activities)}")
        if not rapid_activities.empty:
            avg_completion_percentage = rapid_activities['completion_percentage'].mean().round(2)
            mdFile.new_paragraph(f"Average completion percentage: {avg_completion_percentage}%")
    def get_activity_row(self, task_code: str, start_tasks: pd.DataFrame, end_tasks: pd.DataFrame) -> list:
        start_task = start_tasks[start_tasks['task_code'] == task_code]
        end_task = end_tasks[end_tasks['task_code'] == task_code]

        planned_data = self.get_planned_data(start_task)
        actual_data = self.get_actual_data(end_task)

        return [task_code, actual_data['task_name']] + planned_data + actual_data['actual_data']

    def get_planned_data(self, task: pd.DataFrame) -> List[str]:
        if not task.empty:
            task = task.iloc[0]
            planned_start = self.format_date(task['target_start_date']) or "(new)"
            planned_end = self.format_date(task['target_end_date']) or "(new)"
            if planned_start != "(new)" and planned_end != "(new)":
                planned_duration = (
                        pd.to_datetime(task['target_end_date']) - pd.to_datetime(task['target_start_date'])).days
            else:
                planned_duration = "(new)"
            return [planned_start, planned_end, str(planned_duration)]
        else:
            return ["(new)", "(new)", "(new)"]

    def get_actual_data(self, task: pd.DataFrame) -> Dict[str, Union[str, List[str]]]:
        if not task.empty:
            task = task.iloc[0]
            task_name = task['task_name']
            actual_start = self.format_date(task['act_start_date']) or "(not complete)"
            actual_end = self.format_date(task['act_end_date']) or "(not complete)"

            if actual_start != "(not complete)" and actual_end != "(not complete)":
                actual_duration = (pd.to_datetime(task['act_end_date']) - pd.to_datetime(task['act_start_date'])).days
            else:
                actual_duration = "(not complete)"

            return {
                'task_name': task_name,
                'actual_data': [actual_start, actual_end, str(actual_duration)]
            }
        else:
            return {
                'task_name': "N/A",
                'actual_data': ["(not started)", "(not started)", "(not started)"]
            }

    def generate_window_data_and_progress(self, start_date: Union[str, pd.Timestamp],
                                          end_date: Union[str, pd.Timestamp]) -> Tuple[WindowXER, WindowXER]:
        if self.start_window_xer_folder_path is None or self.end_window_xer_folder_path is None:
            logging.error("Both start and end window XER file paths must be set.")
            raise ValueError("Both start and end window XER file paths must be set.")

        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        start_window = self.process_window(start_date, is_end_window=False)
        end_window = self.process_window(end_date, is_end_window=True)

        self.generate_markdown_report(start_window, end_window, start_date, end_date)

        return start_window, end_window
