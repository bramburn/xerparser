from typing import Tuple, NamedTuple
import pandas as pd
import os
import logging
from xerparser import Xer
from local.libs.xer_file_creation import XerFileGenerator
from local.libs.total_float_method import TotalFloatCPMCalculator
from mdutils import MdUtils
from mdutils.tools import TableOfContents


class WindowXER(NamedTuple):
    xer: Xer
    critical_path: list
    file_path: str


class WindowAnalyzer:
    def __init__(self, xer, start_window_folder_path, end_window_folder_path):
        self.xer = xer
        self.start_window_xer_folder_path = start_window_folder_path
        self.end_window_xer_folder_path = end_window_folder_path
        self.xer_generator = XerFileGenerator(self.xer)

    def process_window(self, date: pd.Timestamp, is_end_window: bool) -> WindowXER:
        window_xer = self.xer_generator.create_modified_copy(date)

        calculator = TotalFloatCPMCalculator(window_xer)
        calculator.set_workday_df(window_xer.workday_df)
        calculator.set_exception_df(window_xer.exception_df)
        critical_path = calculator.calculate_critical_path()
        calculator.update_task_df()

        folder_path = self.end_window_xer_folder_path if is_end_window else self.start_window_xer_folder_path
        window_type = "end" if is_end_window else "start"
        file_name = os.path.join(folder_path, f"{date.strftime('%Y-%m-%d')}_{window_type}_window.xer")
        self.xer_generator.build_xer_file(window_xer, file_name)

        return WindowXER(window_xer, critical_path, file_name)

    def filter_tasks(self, tasks_df, start_date, end_date):
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

    def add_critical_path_comparison(self, mdFile: MdUtils, start_window: WindowXER, end_window: WindowXER):
        mdFile.new_header(level=1, title="Critical Path Comparison")

        # Create sets of critical path task_ids
        start_critical_set = set(start_window.critical_path)
        end_critical_set = set(end_window.critical_path)
        all_critical_tasks = start_critical_set.union(end_critical_set)

        # Table headers
        table_headers = ["Task Code", "Task Name", "Start", "End", "Critical 1*", "Critical 2**"]
        table_data = table_headers.copy()

        for task_id in all_critical_tasks:
            start_task = start_window.xer.task_df[start_window.xer.task_df['task_id'] == task_id].iloc[
                0] if task_id in start_critical_set else None
            end_task = end_window.xer.task_df[end_window.xer.task_df['task_id'] == task_id].iloc[
                0] if task_id in end_critical_set else None

            task = end_task if end_task is not None else start_task
            task_code = task['task_code']
            task_name = task['task_name']

            # Determine start date
            if pd.notnull(task['act_start_date']):
                start_date = f"{task['act_start_date'].strftime('%Y-%m-%d')}A"
            elif pd.notnull(task['target_start_date']):
                start_date = task['target_start_date'].strftime('%Y-%m-%d')
            else:
                start_date = "N/A"

            # Determine end date
            if pd.notnull(task['act_end_date']):
                end_date = f"{task['act_end_date'].strftime('%Y-%m-%d')}A"
            elif pd.notnull(task['target_end_date']):
                end_date = task['target_end_date'].strftime('%Y-%m-%d')
            else:
                end_date = "N/A"

            # Determine criticality
            critical_1 = "True" if task_id in start_critical_set else "False"
            critical_2 = "True" if task_id in end_critical_set else "False"

            table_data.extend([task_code, task_name, start_date, end_date, critical_1, critical_2])

        # Create the table
        num_rows = len(all_critical_tasks) + 1  # +1 for the header row
        mdFile.new_table(columns=6, rows=num_rows, text=table_data, text_align='left')

        # Add footnotes
        mdFile.new_paragraph("*Critical 1: Critical path status at the start of the window")
        mdFile.new_paragraph("**Critical 2: Critical path status at the end of the window")
        mdFile.new_paragraph("A: Actual date")

        # Summary of changes
        added_tasks = end_critical_set - start_critical_set
        removed_tasks = start_critical_set - end_critical_set

        if added_tasks or removed_tasks:
            mdFile.new_header(level=2, title="Changes in Critical Path")
            if added_tasks:
                added_task_codes = [
                    end_window.xer.task_df[end_window.xer.task_df['task_id'] == task_id].iloc[0]['task_code'] for
                    task_id in added_tasks]
                mdFile.new_paragraph(f"Tasks added to critical path: {', '.join(added_task_codes)}")
            if removed_tasks:
                removed_task_codes = [
                    start_window.xer.task_df[start_window.xer.task_df['task_id'] == task_id].iloc[0]['task_code'] for
                    task_id in removed_tasks]
                mdFile.new_paragraph(f"Tasks removed from critical path: {', '.join(removed_task_codes)}")
        else:
            mdFile.new_paragraph("No changes in the critical path between start and end windows.")
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

    def get_planned_data(self, task: pd.DataFrame) -> list:
        if not task.empty:
            task = task.iloc[0]
            planned_start = task['target_start_date'].strftime('%Y-%m-%d')
            planned_end = task['target_end_date'].strftime('%Y-%m-%d')
            planned_duration = (task['target_end_date'] - task['target_start_date']).days
            return [planned_start, planned_end, str(planned_duration)]
        else:
            return ["(new)", "(new)", "(new)"]

    def get_actual_data(self, task: pd.DataFrame) -> dict:
        if not task.empty:
            task = task.iloc[0]
            task_name = task['task_name']
            actual_start = task['act_start_date'].strftime('%Y-%m-%d') if pd.notnull(
                task['act_start_date']) else "(not complete)"

            if pd.notnull(task['act_end_date']):
                actual_end = task['act_end_date'].strftime('%Y-%m-%d')
                actual_duration = (task['act_end_date'] - task['act_start_date']).days
            else:
                actual_end = actual_duration = "(not complete)"

            return {
                'task_name': task_name,
                'actual_data': [actual_start, actual_end, str(actual_duration)]
            }
        else:
            return {
                'task_name': "N/A",
                'actual_data': ["(not started)", "(not started)", "(not started)"]
            }

    def generate_window_data_and_progress(self, start_date: str, end_date: str) -> Tuple[WindowXER, WindowXER]:
        if self.start_window_xer_folder_path is None or self.end_window_xer_folder_path is None:
            logging.error("Both start and end window XER file paths must be set.")
            raise ValueError("Both start and end window XER file paths must be set.")

        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        start_window = self.process_window(start_date, is_end_window=False)
        end_window = self.process_window(end_date, is_end_window=True)

        self.generate_markdown_report(start_window, end_window, start_date, end_date)

        return start_window, end_window
