import pandas as pd
from datetime import datetime

from mdutils import MdUtils
from mdutils.tools.Table import Table

from xerparser import Xer


class ScheduleSplitter:
    def __init__(self, xer, start_date, end_date, split_date):
        self.xer = xer
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.split_date = pd.to_datetime(split_date)
        self.filtered_tasks = None
        self.date_format = "%Y-%m-%d %H:%M"

    def get_processed_df(self):
        """
        Returns the processed DataFrame before filtering.

        Returns:
            pandas.DataFrame: The processed DataFrame containing all tasks.
        """
        if self.xer.task_df is None:
            print("No data has been processed. Please run process_data() first.")
            return None

        return self.xer.task_df

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

    def process_data(self):
        if self.xer.task_df is not None:
            self.xer.task_df['update_date'] = datetime.now().strftime(self.date_format)

            self.xer.task_df['progress'] = self.xer.task_df.apply(lambda row:
                                                                  1.0 if row['act_end_date'] <= self.split_date else
                                                                  0.0 if row['act_start_date'] > self.split_date else
                                                                  (self.split_date - row['act_start_date']) / (
                                                                          row['act_end_date'] - row[
                                                                      'act_start_date'])
                                                                  if pd.notnull(row['act_start_date']) and pd.notnull(
                                                                      row['act_end_date']) else 0.0,
                                                                  axis=1
                                                                  )

            zero_progress_mask = self.xer.task_df['progress'] == 0
            self.xer.task_df.loc[zero_progress_mask, 'act_start_date'] = pd.NaT
            self.xer.task_df.loc[zero_progress_mask, 'act_end_date'] = pd.NaT

            self.xer.update_last_recalc_date(self.split_date)

            # Filter the tasks
            self.filtered_tasks = self.xer.task_df[
                (self.xer.task_df['act_start_date'] >= self.start_date) &
                ((self.xer.task_df['act_end_date'] <= self.end_date) | self.xer.task_df['act_end_date'].isna())]
    def generate_xer(self, output_file):
        date_prefix = self.split_date.strftime("%Y-%m-%d")
        new_filename = f"{date_prefix}_{output_file}"
        xer_contents = self.xer.generate_xer_contents()
        with open(new_filename, 'w', encoding=Xer.CODEC) as f:
            f.write(xer_contents)
        print(f"Updated XER file exported to: {new_filename}")

    def generate_markdown_report(self, output_file):
        if self.filtered_tasks is None:
            print("Please run process_data() before generating the report.")
            return

        project_info = self.xer.project_df.iloc[0]
        project_name = project_info['proj_short_name']
        project_id = project_info['proj_id']

        mdFile = MdUtils(file_name=output_file, title='Project Progress Report')

        # Project Details
        mdFile.new_header(level=2, title='Project Details')
        mdFile.new_list(['**Project Name:** ' + project_name,
                         '**Project ID:** ' + project_id,
                         '**Date Assessed:** ' + self.split_date.strftime('%Y-%m-%d')])

        # Activities in the Period
        mdFile.new_header(level=2, title='Activities in the Period')

        table_data = ['Task ID', 'Task Name', 'Planned Duration (days)', 'Actual Duration', 'Progress']
        duration_changes = []

        for _, row in self.filtered_tasks.iterrows():
            task_code = row['task_code']
            task_name = row['task_name']
            planned_duration = int(row['target_drtn_hr_cnt']) / 8

            if pd.notnull(row['act_start_date']) and pd.notnull(row['act_end_date']):
                actual_duration = (row['act_end_date'] - row['act_start_date']).days
                duration_diff = actual_duration - planned_duration
                if duration_diff != 0:
                    duration_changes.append((task_code, task_name, planned_duration, actual_duration, duration_diff))
            else:
                actual_duration = "N/A"

            progress = f"{row['progress'] * 100:.2f}%"

            table_data.extend([task_code, task_name, f"{planned_duration:.1f}", str(actual_duration), progress])

        mdFile.new_table(columns=5, rows=len(self.filtered_tasks) + 1, text=table_data, text_align='center')

        # Duration Changes in the Period
        mdFile.new_header(level=2, title='Duration Changes in the Period')

        if duration_changes:
            table_data = ['Task ID', 'Task Name', 'Planned Duration (days)', 'Actual Duration (days)',
                          'Difference (days)']
            for task_code, task_name, planned, actual, diff in duration_changes:
                table_data.extend([task_code, task_name, f"{planned:.1f}", str(actual), f"{diff:+.1f}"])

            mdFile.new_table(columns=5, rows=len(duration_changes) + 1, text=table_data, text_align='center')
        else:
            mdFile.new_paragraph("No duration changes in this period.")

        mdFile.create_md_file()
        print(f"Markdown report saved as: {output_file}")