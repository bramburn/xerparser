from datetime import datetime
import numpy as np
import pandas as pd
from xerparser import Xer

date_format = "%Y-%m-%d %H:%M"
asbuilt_file=r"asbuilt.xer"

# define the start and end dates for the filter
start_date = pd.to_datetime('2023-12-01 00:00:00')  # example start date
end_date = pd.to_datetime('2023-12-31 00:00:00')  # example end date
split_date = end_date


def generate_markdown_report(xer, filtered_tasks, split_date):
    """Generate a markdown report with the requested information."""

    # Get project information
    project_info = xer.project_df.iloc[0]
    project_name = project_info['proj_short_name']
    project_id = project_info['proj_id']

    # Create the markdown content
    markdown_content = f"""# Project Progress Report

## Project Details
- **Project Name:** {project_name}
- **Project ID:** {project_id}
- **Date Assessed:** {split_date.strftime('%Y-%m-%d')}

## Activities in the Period

| Task ID | Task Name | Planned Duration (days) | Actual Duration | Progress |
|---------|-----------|-------------------------|-----------------|----------|
"""

    duration_changes = []

    for _, row in filtered_tasks.iterrows():
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

        markdown_content += f"| {task_code} | {task_name} | {planned_duration:.1f} | {actual_duration} | {progress} |\n"

    markdown_content += "\n## Duration Changes in the Period\n\n"
    markdown_content += "| Task ID | Task Name | Planned Duration (days) | Actual Duration (days) | Difference (days) |\n"
    markdown_content += "|---------|-----------|-------------------------|------------------------|--------------------|\n"

    for task_code, task_name, planned, actual, diff in duration_changes:
        markdown_content += f"| {task_code} | {task_name} | {planned:.1f} | {actual} | {diff:+.1f} |\n"

    # Save the markdown report
    report_filename = f"{split_date.strftime('%Y-%m-%d')}_progress_report.md"
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(markdown_content)

    print(f"Markdown report saved as: {report_filename}")
def calculate_duration(start_date, end_date, day_hr_cnt):
    actual_start = datetime.strptime(start_date, date_format)
    actual_finish = datetime.strptime(end_date, date_format)
    actual_duration = actual_finish - actual_start
    return (actual_duration.days * 24 + actual_duration.seconds / 3600) * day_hr_cnt

def export_xer(xer, output_file, split_date):
    """Export the updated data as a new XER file with the split date in the filename."""
    date_prefix = split_date.strftime("%Y-%m-%d")
    new_filename = f"{date_prefix}_{output_file}"
    xer_contents = xer.generate_xer_contents()
    with open(new_filename, 'w', encoding=Xer.CODEC) as f:
        f.write(xer_contents)
    print(f"Updated XER file exported to: {new_filename}")

def main():
    file = asbuilt_file
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)
    calendars = {}
    if xer.calendar_df is not None and xer.taskpred_df is not None:
        for clndr_id, day_hr_cnt in zip(xer.calendar_df['clndr_id'], xer.calendar_df['day_hr_cnt']):
            calendars[clndr_id] = day_hr_cnt

    if xer.task_df is not None:
        # update the table value of update_date to today's date
        xer.task_df['update_date'] = datetime.now().strftime(date_format)

        # Update progress calculation based on split_date
        xer.task_df['progress'] = xer.task_df.apply(lambda row:
            1.0 if row['act_end_date'] <= split_date else
            0.0 if row['act_start_date'] > split_date else
            (split_date - row['act_start_date']) / (row['act_end_date'] - row['act_start_date'])
            if pd.notnull(row['act_start_date']) and pd.notnull(row['act_end_date']) else 0.0,
            axis=1
        )

        # For tasks with zero progress, set actual start and end dates to NaT (Not a Time)
        zero_progress_mask = xer.task_df['progress'] == 0
        xer.task_df.loc[zero_progress_mask, 'act_start_date'] = pd.NaT
        xer.task_df.loc[zero_progress_mask, 'act_end_date'] = pd.NaT

        # Update the project's last_recalc_date
        xer.update_last_recalc_date(split_date)

        # Export the updated data as a new XER file
        export_xer(xer, "updated_output.xer", split_date)



        # filter the dataframe based on the start and end date
        filtered_tasks = xer.task_df[
            (xer.task_df['act_start_date'] >= start_date) &
            ((xer.task_df['act_end_date'] <= end_date) | xer.task_df['act_end_date'].isna())]
        # After processing the data and creating filtered_tasks, add:
        generate_markdown_report(xer, filtered_tasks, split_date)
        # print the required data for the filtered tasks
        for i, row in filtered_tasks.iterrows():
            task_code = row['task_code']
            task_name = row['task_name']
            proj_id = row['proj_id']
            target_drtn_hr_cnt = row['target_drtn_hr_cnt']
            act_start_date = row['act_start_date']
            act_end_date = row['act_end_date']
            progress = row['progress']

            # calculate the actual completion duration
            actual_completion_duration = act_end_date - act_start_date if pd.notnull(act_start_date) and pd.notnull(act_end_date) else pd.NaT

            print(
                f"task id: {task_code}, {task_name}, proj: {proj_id}, planned duration {int(target_drtn_hr_cnt) / 8} days, actual completion: {actual_completion_duration} [start : {act_start_date} end: {act_end_date}] - {progress * 100:.2f}%")
    else:
        print("No task data found in the XER file.")

if __name__ == "__main__":
    main()