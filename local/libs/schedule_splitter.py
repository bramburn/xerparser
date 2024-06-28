import logging
import os
from typing import Tuple

import pandas as pd
from datetime import datetime

from mdutils import MdUtils
from mdutils.tools.Table import Table

from local.libs.xer_file_creation import XerFileGenerator, ProgressCalculator
from xerparser import Xer


class ScheduleSplitter:
    def __init__(self, xer):
        self.end_window_xer_folder_path = None
        self.start_window_xer_folder_path = None
        self.xer = xer
        self.modified_xer = None
        self.filtered_tasks = None
        self.date_format = "%Y-%m-%d %H:%M"
        self.start_date = None
        self.end_date = None
        self.split_date = None

    def get_filtered_df(self):
        """
        Returns the filtered DataFrame.

        Returns:
            pandas.DataFrame: The filtered DataFrame containing tasks within the specified date range.
        """
        if self.filtered_tasks is None:
            print("No filtered data available. Please run process_data() first.")
            return None

        return self.filtered_tasks

    def process_data(self, start_date: str, end_date: str, split_date: str) -> Tuple[Xer, pd.DataFrame]:
        """
        Process the XER data, create a modified copy, and filter tasks.

        Args:
            start_date (str): The start date for filtering tasks.
            end_date (str): The end date for filtering tasks.
            split_date (str): The date to split the schedule and calculate progress.

        Returns:
            Tuple[Xer, pd.DataFrame]: A tuple containing the modified Xer object and the filtered tasks DataFrame.
        """
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.split_date = pd.to_datetime(split_date)

        xer_generator = XerFileGenerator(self.xer)
        self.modified_xer = xer_generator.create_modified_copy(self.split_date)

        # Filter the tasks
        if self.modified_xer.task_df is not None:
            self.filtered_tasks = self.filter_tasks(self.modified_xer.task_df, self.start_date, self.end_date)
        else:
            self.filtered_tasks = pd.DataFrame()

        return self.modified_xer, self.filtered_tasks

    def set_start_window_xer_filepath(self, start_window_xer_folder_path: str):
        if os.path.isdir(start_window_xer_folder_path):
            self.start_window_xer_folder_path = start_window_xer_folder_path
        else:
            raise ValueError("The provided path is not a valid directory.")

    def set_end_window_xer_filepath(self, end_window_xer_foler_path: str):
        if os.path.isdir(end_window_xer_foler_path):
            self.end_window_xer_folder_path = end_window_xer_foler_path

    def generate_window_data_and_progress(self, start_date: str, end_date: str) -> Tuple[Xer, pd.DataFrame]:

        if self.start_window_xer_folder_path is None or self.end_window_xer_filepath is None:
            logging.error("Both start and end window XER file paths must be set.")
            raise ValueError("Both start and end window XER file paths must be set.")

        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        xer_generator = XerFileGenerator(self.xer)
        # process the late one first
        end_window_xer = xer_generator.create_modified_copy(end_date)
        # save file
        date_prefix = self.end_date.strftime("%Y-%m-%d")
        file_name = os.path.join(self.end_window_xer_folder_path, f"{date_prefix}_end_window.xer")
        xer_generator.generate_xer_file(self.end_window_xer_folder_path, date_prefix)

        # update the progress to the start window
        start_window_xer = xer_generator.create_modified_copy(start_date)

        # save file
        date_prefix = self.start_date.strftime("%Y-%m-%d")
        xer_generator.generate_xer_file(self.start_window_xer_folder_path, date_prefix)

        # now we need to produce the report for the start and end window

        return (start_window_xer, end_window_xer)

        # compare the windows milestone and activities planned vs complete

    def filter_tasks(self, tasks_df, start_date, end_date):
        return tasks_df[
            (tasks_df['act_start_date'] >= start_date) &
            ((tasks_df['act_end_date'] <= end_date) | tasks_df['act_end_date'].isna())
            ]

    def generate_xer(self, output_file):
        if self.modified_xer is None:
            print("Please run process_data() before generating the XER file.")
            return

        xer_generator = XerFileGenerator(self.modified_xer)
        date_prefix = self.split_date.strftime("%Y-%m-%d")
        xer_generator.generate_xer_file(output_file, date_prefix)

    def generate_markdown_report(self, output_file):
        if self.filtered_tasks is None:
            print("Please run process_data() before generating the report.")
            return

        project_info = self.xer.project_df
        mdFile = MdUtils(file_name=output_file, title='Project Progress Report')

        # Table of Contents
        mdFile.new_table_of_contents(table_title='Contents', depth=2)

        # Project Report Header
        mdFile.new_header(level=1, title="Project Report")

        # Projects
        if not project_info.empty:
            mdFile.new_header(level=2, title='Projects')
            table_data = ["Project Name", "Project ID"]
            if 'proj_short_name' in project_info.columns and 'proj_id' in project_info.columns:
                for _, row in project_info.iterrows():
                    project_name = row['proj_short_name'] if pd.notna(row['proj_short_name']) else 'N/A'
                    project_id = row['proj_id'] if pd.notna(row['proj_id']) else 'N/A'
                    table_data.extend([project_name, project_id])
            mdFile.new_table(columns=2, rows=len(project_info) + 1, text=table_data, text_align='left')
        else:
            mdFile.new_paragraph("No Project information found")

        # Current Project Details
        mdFile.new_header(level=2, title='Current Project Details')
        project_name = project_info.iloc[0]['proj_short_name']
        project_id = project_info.iloc[0]['proj_id']
        mdFile.new_list(['**Project Name:** ' + project_name,
                         '**Project ID:** ' + project_id,
                         '**Date Assessed:** ' + self.split_date.strftime('%Y-%m-%d')])

        # Milestones
        mdFile.new_header(level=2, title='Milestones')
        milestone_data = ["Task ID", "Milestone Name", "Planned Start", "Planned End", "Actual Start", "Actual End"]
        for _, row in self.xer.task_df.iterrows():
            if row['task_type'] in ['TT_Mile', 'TT_FinMile']:
                milestone_data.extend([
                    row['task_code'],
                    row['task_name'],
                    pd.to_datetime(row['target_start_date']).strftime('%Y-%m-%d') if pd.notna(
                        row['target_start_date']) else 'N/A',
                    pd.to_datetime(row['target_end_date']).strftime('%Y-%m-%d') if pd.notna(
                        row['target_end_date']) else 'N/A',
                    pd.to_datetime(row['act_start_date']).strftime('%Y-%m-%d') if pd.notna(
                        row['act_start_date']) else 'N/A',
                    pd.to_datetime(row['act_end_date']).strftime('%Y-%m-%d') if pd.notna(row['act_end_date']) else 'N/A'
                ])
        mdFile.new_table(columns=6, rows=len(milestone_data) // 6, text=milestone_data, text_align='center')

        # Activities in the Period
        mdFile.new_header(level=2, title='Activities in the Period')
        activities_data = ["Task ID", "Task Name", "Planned Duration (days)", "Actual Duration", "Progress"]
        duration_changes = []
        for _, row in self.filtered_tasks.iterrows():
            planned_duration = int(row['target_drtn_hr_cnt']) / 8
            if pd.notnull(row['act_start_date']) and pd.notnull(row['act_end_date']):
                actual_duration = (row['act_end_date'] - row['act_start_date']).days
                duration_diff = actual_duration - planned_duration
                if duration_diff != 0:
                    duration_changes.append(
                        (row['task_code'], row['task_name'], planned_duration, actual_duration, duration_diff))
            else:
                actual_duration = "N/A"
            progress = f"{row['progress'] * 100:.2f}%"
            activities_data.extend(
                [row['task_code'], row['task_name'], f"{planned_duration:.1f}", str(actual_duration), progress])
        mdFile.new_table(columns=5, rows=len(self.filtered_tasks) + 1, text=activities_data, text_align='center')

        # Duration Changes in the Period
        mdFile.new_header(level=2, title='Duration Changes in the Period')
        if duration_changes:
            changes_data = ["Task ID", "Task Name", "Planned Duration (days)", "Actual Duration (days)",
                            "Difference (days)"]
            for task_code, task_name, planned, actual, diff in duration_changes:
                changes_data.extend([task_code, task_name, f"{planned:.1f}", str(actual), f"{diff:+.1f}"])
            mdFile.new_table(columns=5, rows=len(duration_changes) + 1, text=changes_data, text_align='center')
        else:
            mdFile.new_paragraph("No duration changes in this period.")

        mdFile.create_md_file()
        print(f"Markdown report saved as: {output_file}")
