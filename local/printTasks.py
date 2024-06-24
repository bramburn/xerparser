import numpy as np
from datetime import datetime, timedelta

import pandas as pd

from xerparser import Xer

date_format = "%Y-%m-%d %H:%M"


def main():
    file = r"baseline.xer"
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)
    calendars = {}
    if xer.calendar_df is not None:

        for clndr_id, day_hr_cnt in zip(xer.calendar_df['clndr_id'], xer.calendar_df['day_hr_cnt']):
            calendars[clndr_id] = day_hr_cnt

    if xer.task_df is not None:
        print("\nFirst 100 Tasks:")
        print("task_code, task_name, proj_id")
        for i, (task_code, task_name, proj_id, target_drtn_hr_cnt, clndr_id, act_start_date, act_end_date) in enumerate(
                zip(xer.task_df['task_code'][:100], xer.task_df['task_name'][:100], xer.task_df['proj_id'][:100],
                    xer.task_df['target_drtn_hr_cnt'][:100], xer.task_df['clndr_id'][:100]
                    , xer.task_df['act_start_date'][:100]
                    , xer.task_df['act_end_date'][:100]
                    )):
            actual_start = datetime.strptime(act_start_date, date_format)
            actual_finish = datetime.strptime(act_end_date, date_format)
            actual_duration = actual_finish - actual_start
            planned_duration = int(target_drtn_hr_cnt) / int(calendars[clndr_id])
            print(
                f"task id: {task_code}, {task_name}, proj: {proj_id}, planned duration {planned_duration} days, actual {actual_duration.days}")
    else:
        print("No task data found in the XER file.")


if __name__ == "__main__":
    main()
