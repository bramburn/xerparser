import os
from datetime import datetime
import pandas as pd

from local.libs.schedule_splitter import ScheduleSplitter
from xerparser import Xer

date_format = "%Y-%m-%d %H:%M"
asbuilt_file = r"asbuilt.xer"

# define the start and end dates for the filter
start_date = '2023-12-01 00:00:00'  # example start date
end_date = '2023-12-31 00:00:00'  # example end date
split_date = end_date

def main():
    file = asbuilt_file
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)

    # Create a ScheduleSplitter instance
    splitter = ScheduleSplitter(xer, start_date, end_date, split_date)

    # Process the data
    splitter.process_data()

    # Get the filtered DataFrame
    filtered_tasks = splitter.get_filtered_df()

    # Generate the updated XER file
    date_prefix = splitter.split_date.strftime("%Y-%m-%d")
    asbuilt_filename = os.path.splitext(asbuilt_file)[0]  # Get filename without extension
    output_xer_filename = f"{date_prefix}_{asbuilt_filename}_updated.xer"
    splitter.generate_xer(output_xer_filename)

    # Generate the markdown report
    splitter.generate_markdown_report(f"{date_prefix}_{asbuilt_filename}_report.md")

    # Print details for filtered tasks (optional)
    if filtered_tasks is not None:
        for i, row in filtered_tasks.iterrows():
            task_code = row['task_code']
            task_name = row['task_name']
            proj_id = row['proj_id']
            target_drtn_hr_cnt = row['target_drtn_hr_cnt']
            act_start_date = row['act_start_date']
            act_end_date = row['act_end_date']
            progress = row['progress']

            actual_completion_duration = act_end_date - act_start_date if pd.notnull(
                act_start_date) and pd.notnull(act_end_date) else pd.NaT

            print(
                f"task id: {task_code}, {task_name}, proj: {proj_id}, planned duration {int(target_drtn_hr_cnt) / 8} days, actual completion: {actual_completion_duration} [start : {act_start_date} end: {act_end_date}] - {progress * 100:.2f}%")
    else:
        print("No task data found in the XER file.")

if __name__ == "__main__":
    main()