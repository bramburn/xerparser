import numpy as np
from datetime import datetime, timedelta
import pandas as pd

from local.critical_path_class import CriticalPathCalculator
from xerparser import Xer

date_format = "%Y-%m-%d %H:%M"

# split_date = pd.Timestamp('2023-12-01 00:00:00')
# define the start and end dates for the filter
start_date = pd.to_datetime('2022-11-01 00:00:00')  # example start date
end_date = pd.to_datetime('2023-01-01 00:00:00')  # example end date
split_date = end_date


def calculate_duration(start_date, end_date, day_hr_cnt):
    actual_start = datetime.strptime(start_date, date_format)
    actual_finish = datetime.strptime(end_date, date_format)
    actual_duration = actual_finish - actual_start
    return (actual_duration.days * 24 + actual_duration.seconds / 3600) * day_hr_cnt


def main():
    file = r"updated.xer"
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

        # filter out activities that have not started yet or have finished after the specific split date
        xer.task_df = xer.task_df[(xer.task_df['act_start_date'] <= split_date.strftime(date_format)) & (
                xer.task_df['act_end_date'] <= split_date.strftime(date_format))]

        # set the progress of activities that have not started or have finished after the split date to 0%
        xer.task_df.loc[(xer.task_df['act_start_date'] > split_date.strftime(date_format)) | (
                xer.task_df['act_end_date'] > split_date.strftime(date_format)), 'progress'] = 0

        # calculate the progress of activities that have started before the split date and completed after the split date
        for i, (task_code, task_name, proj_id, target_drtn_hr_cnt, clndr_id, act_start_date, act_end_date) in enumerate(
                zip(xer.task_df['task_code'], xer.task_df['task_name'], xer.task_df['proj_id'],
                    xer.task_df['target_drtn_hr_cnt'], xer.task_df['clndr_id']
                    , xer.task_df['act_start_date']
                    , xer.task_df['act_end_date'])):
            if act_start_date <= split_date and act_end_date > split_date:
                actual_duration_before_split = calculate_duration(act_start_date, split_date, calendars[clndr_id])
                total_duration = calculate_duration(act_start_date, act_end_date, calendars[clndr_id])
                progress = actual_duration_before_split / total_duration
                xer.task_df.loc[i, 'progress'] = progress
            elif act_start_date <= split_date and act_end_date <= split_date:
                pass  # do nothing, progress is already correct
            else:
                # set the progress of activities that have started before the split date but did not complete after the split date based on the original planned duration
                progress = target_drtn_hr_cnt / calendars[clndr_id]
                xer.task_df.loc[i, 'progress'] = progress
        # print the first 100 tasks
        # set the act_end_date to NaN for tasks that have not been completed by the split_date

        # vectorising and cleaning up the act_end_date
        xer.task_df.loc[(xer.task_df['act_start_date'] <= split_date) & (
                xer.task_df['act_end_date'] > split_date), 'act_end_date'] = np.nan

        # Extract the tasks and taskpred information
        tasks_df = xer.task_df[
            ['act_start_date', 'act_end_date', 'task_code', 'task_name', 'proj_id', 'target_drtn_hr_cnt', 'duration',
             'progress', 'remaining_days', 'early_start', 'early_finish', 'late_start', 'late_finish']]
        tasks_df.loc[:, 'task_id'] = tasks_df.index  # Assuming task_id is the index

        taskpred_df = xer.taskpred_df[['pred_task_id', 'task_id', 'lag_days', 'pred_type']]  # Add 'pred_type' column

        # Create an instance of the CriticalPathCalculator and run the calculations
        cpc = CriticalPathCalculator(tasks_df, taskpred_df)
        critical_tasks, critical_relationships, total_critical_path_duration = cpc.run()

        # Print the critical path tasks
        print("Critical Path Tasks:")
        for _, task in critical_tasks.iterrows():
            print(f"Task: {task['task_code']} - {task['task_name']}")
            print(f"  Project: {task['proj_id']}")
            print(f"  Duration: {task['duration']} days")
            print(f"  Early Start: {task['early_start']:.2f}, Early Finish: {task['early_finish']:.2f}")
            print(f"  Late Start: {task['late_start']:.2f}, Late Finish: {task['late_finish']:.2f}")
            print()

        # Print the critical path relationships
        print("Critical Path Relationships:")
        print(critical_relationships)
        for _, rel in critical_relationships.iterrows():
            from_task = critical_tasks[critical_tasks['task_id'] == rel['from_task']].iloc[0]
            to_task = critical_tasks[critical_tasks['task_id'] == rel['to_task']].iloc[0]
            print(f"From: {from_task['task_code']} - {from_task['task_name']}")
            print(f"To: {to_task['task_code']} - {to_task['task_name']}")
            print(f"  Relationship: {rel['relationship']}, Lag: {rel['lag']} days")
            print()

        print(f"Total Critical Path Duration: {total_critical_path_duration:.2f} hours")

        # filter the dataframe based on the start and end date
        filtered_tasks = xer.task_df[
            (xer.task_df['act_start_date'] >= start_date) &
            ((xer.task_df['act_end_date'] <= end_date) | xer.task_df['act_end_date'].isna())]

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
            actual_completion_duration = act_end_date - act_start_date

            print(
                f"task id: {task_code}, {task_name}, proj: {proj_id}, planned duration {int(target_drtn_hr_cnt) / 8} days, actual completion: {actual_completion_duration} [start : {act_start_date} end: {act_end_date}] - {progress * 100:2f}%")
    else:
        print("No task data found in the XER file.")


if __name__ == "__main__":
    main()
