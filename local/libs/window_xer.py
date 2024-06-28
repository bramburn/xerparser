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

        start_critical_set: Set[str] = set(start_window.critical_path)
        end_critical_set: Set[str] = set(end_window.critical_path)
        all_critical_tasks: Set[str] = start_critical_set.union(end_critical_set)

        table_headers = ["Task Code", "Task Name", "Start", "End", "Critical 1*", "Critical 2**"]
        table_data = table_headers.copy()

        for task_id in all_critical_tasks:
            start_task = start_window.xer.task_df[start_window.xer.task_df['task_id'] == task_id].iloc[
                0] if task_id in start_critical_set else None
            end_task = end_window.xer.task_df[end_window.xer.task_df['task_id'] == task_id].iloc[
                0] if task_id in end_critical_set else None

            task: pd.Series = end_task if end_task is not None else start_task
            task_code = task['task_code']
            task_name = task['task_name']

            start_date = self.format_date(task['act_start_date'], 'A') or self.format_date(
                task['target_start_date']) or "N/A"
            end_date = self.format_date(task['act_end_date'], 'A') or self.format_date(task['target_end_date']) or "N/A"

            critical_1 = "True" if task_id in start_critical_set else "False"
            critical_2 = "True" if task_id in end_critical_set else "False"

            table_data.extend([task_code, task_name, start_date, end_date, critical_1, critical_2])

        num_rows = len(all_critical_tasks) + 1
        md_file_utils.new_table(columns=6, rows=num_rows, text=table_data, text_align='left')

        md_file_utils.new_paragraph("*Critical 1: Critical path status at the start of the window")
        md_file_utils.new_paragraph("**Critical 2: Critical path status at the end of the window")
        md_file_utils.new_paragraph("A: Actual date")

        # Add footnotes
        md_file_utils.new_paragraph("*Critical 1: Critical path status at the start of the window")
        md_file_utils.new_paragraph("**Critical 2: Critical path status at the end of the window")
        md_file_utils.new_paragraph("A: Actual date")

        # Summary of changes
        added_tasks = end_critical_set - start_critical_set
        removed_tasks = start_critical_set - end_critical_set

        if added_tasks or removed_tasks:
            md_file_utils.new_header(level=2, title="Changes in Critical Path")
            if added_tasks:
                added_task_codes = [
                    end_window.xer.task_df[end_window.xer.task_df['task_id'] == task_id].iloc[0]['task_code'] for
                    task_id in added_tasks]
                md_file_utils.new_paragraph(f"Tasks added to critical path: {', '.join(added_task_codes)}")
            if removed_tasks:
                removed_task_codes = [
                    start_window.xer.task_df[start_window.xer.task_df['task_id'] == task_id].iloc[0]['task_code'] for
                    task_id in removed_tasks]
                md_file_utils.new_paragraph(f"Tasks removed from critical path: {', '.join(removed_task_codes)}")
        else:
            md_file_utils.new_paragraph("No changes in the critical path between start and end windows.")

    def add_activities_in_period(self, mdFile: MdUtils, start_window: WindowXER, end_window: WindowXER,
                                 start_date: pd.Timestamp, end_date: pd.Timestamp):
        mdFile.new_header(level=1, title="Activities in the Period")

        start_tasks = self.filter_tasks(start_window.xer.task_df, start_date, end_date)
        end_tasks = self.filter_tasks(end_window.xer.task_df, start_date, end_date)

        all_task_codes = set(start_tasks['task_code']).union(set(end_tasks['task_code']))

        activities_table = ["Task Code", "Task Name", "Planned Start", "Planned End", "Planned Duration",
                            "Actual Start", "Actual End", "Actual Duration"]

        for task_code in all_task_codes:
            row = self.get_activity_row(task_code, start_tasks, end_tasks)
            activities_table.extend(row)

        mdFile.new_table(columns=8, rows=len(all_task_codes) + 1, text=activities_table, text_align='left')

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
