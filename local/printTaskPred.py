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

    if xer.taskpred_df is not None:
        print("\nAll Task Predecessors:")
        print("pred_task_code, succ_task_code, lag_days")
        for pred_task_code, succ_task_code, lag_days in zip(xer.taskpred_df['pred_task_id'],
                                                            xer.taskpred_df['pred_type'],
                                                            xer.taskpred_df['lag_days']):
            print(f"{pred_task_code}, {succ_task_code}, {lag_days}")
    else:
        print("No task predecessor data found in the XER file.")


if __name__ == "__main__":
    main()
