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
    def __init__(self, xer: Xer, report_folder_path: str):
        """
        Initializes a new instance of the WindowAnalyzer class.

        Args:
            xer (Xer): The Xer object representing the original XER file.
            report_folder_path (str): The path to the folder where the start window XER files will be saved.


        Returns:
            None
        """
        self.xer = xer
        self.report_xer_folder_path = report_folder_path
        self.xer_generator = XerFileGenerator(self.xer)
        self.monitored_tasks = None

    def set_monitored_tasks(self, task_codes: List[str]):
        self.monitored_tasks = task_codes

    def generate_monitored_tasks_report(self, mdFile: MdUtils, start_window: WindowXER, end_window: WindowXER):
        if not self.monitored_tasks:
            return

        mdFile.new_header(level=1, title="Monitored Tasks Report")

        table_headers = ["Task Code", "Task Name", "Start (Start)", "Finish (Start)",
                         "Start (End)", "Finish (End)", "Start Date Difference", "Finish Date Difference"]
        table_data = table_headers.copy()

        def get_date(task, field):
            return pd.to_datetime(task[field]) if pd.notnull(task[field]) else None

        for task_code in self.monitored_tasks:
            start_task = start_window.xer.task_df[start_window.xer.task_df['task_code'] == task_code]
            end_task = end_window.xer.task_df[end_window.xer.task_df['task_code'] == task_code]

            if start_task.empty or end_task.empty:
                continue

            start_task = start_task.iloc[0]
            end_task = end_task.iloc[0]

            # Get dates for calculations
            start_start_date = get_date(start_task, 'act_start_date') or get_date(start_task, 'target_start_date')
            start_finish_date = get_date(start_task, 'act_end_date') or get_date(start_task, 'target_end_date')
            end_start_date = get_date(end_task, 'act_start_date') or get_date(end_task, 'target_start_date')
            end_finish_date = get_date(end_task, 'act_end_date') or get_date(end_task, 'target_end_date')

            # Calculate differences
            def calculate_difference(date1, date2):
                if date1 and date2:
                    diff = (date2 - date1).days
                    return f"{diff} days"
                return "N/A"

            start_diff = calculate_difference(start_start_date, end_start_date)
            finish_diff = calculate_difference(start_finish_date, end_finish_date)

            # Format dates for display
            def format_display_date(date, is_actual):
                if pd.isnull(date):
                    return "N/A"
                formatted = date.strftime('%Y-%m-%d')
                return f"{formatted}A" if is_actual else formatted

            start_start_display = format_display_date(start_start_date, pd.notnull(start_task['act_start_date']))
            start_finish_display = format_display_date(start_finish_date, pd.notnull(start_task['act_end_date']))
            end_start_display = format_display_date(end_start_date, pd.notnull(end_task['act_start_date']))
            end_finish_display = format_display_date(end_finish_date, pd.notnull(end_task['act_end_date']))

            table_data.extend([
                task_code,
                start_task['task_name'],
                start_start_display,
                start_finish_display,
                end_start_display,
                end_finish_display,
                start_diff,
                finish_diff
            ])

        num_rows = len(table_data) // 8  # 8 is the new number of columns
        mdFile.new_table(columns=8, rows=num_rows, text=table_data, text_align='left')

        mdFile.new_paragraph("Note: Dates marked with 'A' indicate actual dates.")
        mdFile.new_paragraph(
            "Start Date Difference: The number of days between the start dates in the start and end windows.")
        mdFile.new_paragraph(
            "Finish Date Difference: The number of days between the finish dates in the start and end windows.")
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

        folder_path = self.report_xer_folder_path if is_end_window else self.report_xer_folder_path
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
        file_name = os.path.join(self.report_xer_folder_path,
                                 f"window_analysis_report_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}")

        mdFile = MdUtils(
            file_name=file_name,
            title="Window Analysis Report")

        self.add_table_of_contents(mdFile)
        self.add_window_period(mdFile, start_date, end_date)
        # Add the monitored tasks report right after the window period
        if self.monitored_tasks:
            self.generate_monitored_tasks_report(mdFile, start_window, end_window)
            self.generate_monitored_tasks_impact_report(mdFile, start_window, end_window)

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

            start_date = self.format_date(task['act_start_date'], suffix='A') or self.format_date(
                task['target_start_date']) or "N/A"
            end_date = self.format_date(task['act_end_date'], suffix='A') or self.format_date(
                task['target_end_date']) or "N/A"

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

    def calculate_duration(self, start_date: Union[str, pd.Timestamp], end_date: Union[str, pd.Timestamp]) -> int:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        return (end - start).days

    def generate_monitored_tasks_impact_report(self, mdFile: MdUtils, start_window: WindowXER, end_window: WindowXER):
        if not self.monitored_tasks:
            return

        mdFile.new_header(level=1, title="Monitored Tasks Impact Report")

        table_headers = ["Impacting Task Code", "Impacting Task Name", "Planned Duration", "Actual Duration",
                         "Delay", "Affected Monitored Task", "Monitored Task Date Change"]
        table_data = table_headers.copy()

        for task_code in self.monitored_tasks:
            start_task = start_window.xer.task_df[start_window.xer.task_df['task_code'] == task_code].iloc[0]
            end_task = end_window.xer.task_df[end_window.xer.task_df['task_code'] == task_code].iloc[0]

            start_date = pd.to_datetime(start_task['act_end_date'] or start_task['target_end_date'])
            end_date = pd.to_datetime(end_task['act_end_date'] or end_task['target_end_date'])

            if start_date != end_date:
                # Find impacting tasks
                impacting_tasks = self.find_impacting_tasks(start_window, end_window, task_code)

                for imp_task in impacting_tasks:
                    imp_start = start_window.xer.task_df[start_window.xer.task_df['task_id'] == imp_task].iloc[0]
                    imp_end = end_window.xer.task_df[end_window.xer.task_df['task_id'] == imp_task].iloc[0]

                    planned_duration = self.calculate_duration(
                        imp_start['target_start_date'],
                        imp_start['target_end_date']
                    )
                    actual_duration = self.calculate_duration(
                        imp_end['act_start_date'] or imp_end['target_start_date'],
                        imp_end['act_end_date'] or imp_end['target_end_date']
                    )
                    delay = actual_duration - planned_duration

                    start_date_str = self.format_date(start_task['act_end_date'], suffix='A') or self.format_date(
                        start_task['target_end_date'])
                    end_date_str = self.format_date(end_task['act_end_date'], suffix='A') or self.format_date(
                        end_task['target_end_date'])

                    table_data.extend([
                        imp_end['task_code'],
                        imp_end['task_name'],
                        f"{planned_duration} days",
                        f"{actual_duration} days",
                        f"{delay} days",
                        task_code,
                        f"{start_date_str} -> {end_date_str}"
                    ])

        num_rows = len(table_data) // 7  # 7 is the number of columns
        mdFile.new_table(columns=7, rows=num_rows, text=table_data, text_align='left')
    def find_impacting_tasks(self, start_window: WindowXER, end_window: WindowXER, monitored_task_code: str) -> List[
        str]:
        start_task = start_window.xer.task_df[start_window.xer.task_df['task_code'] == monitored_task_code].iloc[0]
        end_task = end_window.xer.task_df[end_window.xer.task_df['task_code'] == monitored_task_code].iloc[0]

        start_predecessors = set(
            start_window.xer.taskpred_df[start_window.xer.taskpred_df['task_id'] == start_task['task_id']][
                'pred_task_id'])
        end_predecessors = set(
            end_window.xer.taskpred_df[end_window.xer.taskpred_df['task_id'] == end_task['task_id']]['pred_task_id'])

        all_predecessors = start_predecessors.union(end_predecessors)

        impacting_tasks = []
        for pred in all_predecessors:
            start_pred = start_window.xer.task_df[start_window.xer.task_df['task_id'] == pred].iloc[0]
            end_pred = end_window.xer.task_df[end_window.xer.task_df['task_id'] == pred].iloc[0]

            start_duration = self.calculate_duration(start_pred['target_start_date'], start_pred['target_end_date'])
            end_duration = self.calculate_duration(
                end_pred['act_start_date'] or end_pred['target_start_date'],
                end_pred['act_end_date'] or end_pred['target_end_date']
            )

            if end_duration > start_duration:
                impacting_tasks.append(pred)

        return impacting_tasks

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

        # Updated table: Activities Started in the Period (but not completed)
        mdFile.new_header(level=2, title="Activities Started in the Period (In Progress)")
        started_activities = end_window.xer.task_df[
            (end_window.xer.task_df['act_start_date'] >= start_date) &
            (end_window.xer.task_df['act_start_date'] <= end_date) &
            (pd.isnull(end_window.xer.task_df['act_end_date']) | (end_window.xer.task_df['act_end_date'] > end_date))
            ]

        started_table = ["Task Code", "Task Name", "Actual Start", "Planned End", "Planned % Complete",
                         "Actual % Complete"]
        for _, task in started_activities.iterrows():
            planned_duration = (
                        pd.to_datetime(task['target_end_date']) - pd.to_datetime(task['target_start_date'])).days
            actual_duration_to_date = (end_date - pd.to_datetime(task['act_start_date'])).days
            temp_actual_duration = task.get('temp_actual_duration', actual_duration_to_date)

            planned_progress = min(actual_duration_to_date / planned_duration, 1) * 100 if planned_duration > 0 else 0
            actual_progress = min(temp_actual_duration / planned_duration, 1) * 100 if planned_duration > 0 else 0

            started_table.extend([
                task['task_code'],
                task['task_name'],
                self.format_date(task['act_start_date']) or "N/A",
                self.format_date(task['target_end_date']) or "N/A",
                f"{planned_progress:.2f}%",
                f"{actual_progress:.2f}%"
            ])

        mdFile.new_table(columns=6, rows=len(started_activities) + 1, text=started_table, text_align='left')

        # Add explanation for the percentages
        mdFile.new_paragraph(
            "* Planned % Complete: Based on the planned duration and the time elapsed since the actual start date up to the end of the analysis window.")
        mdFile.new_paragraph(
            "* Actual % Complete: Based on the actual progress of the task as of the end date of the analysis window.")



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
        if self.report_xer_folder_path is None or self.report_xer_folder_path is None:
            logging.error("Both start and end window XER file paths must be set.")
            raise ValueError("Both start and end window XER file paths must be set.")

        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        start_window = self.process_window(start_date, is_end_window=False)
        end_window = self.process_window(end_date, is_end_window=True)

        self.generate_markdown_report(start_window, end_window, start_date, end_date)

        return start_window, end_window
