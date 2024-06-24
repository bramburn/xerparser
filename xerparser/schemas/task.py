# task.py

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List

import numpy as np
import pandas as pd


class TaskFields(Enum):
    TASK_ID = "Unique ID"
    TASK_CODE = "Activity ID"
    TASK_NAME = "Activity Name"
    PROJ_ID = "Project"
    WBS_ID = "WBS"
    CLNDR_ID = "Calendar"
    ACT_START_DATE = "Actual Start"
    ACT_END_DATE = "Actual Finish"
    ACT_WORK_QTY = "Actual Labor Units"
    ACT_EQUIP_QTY = "Actual Nonlabor Units"
    ACT_THIS_PER_WORK_QTY = "Actual This Period Labor Units"
    ACT_THIS_PER_EQUIP_QTY = "Actual This Period Nonlabor Units"
    AUTO_COMPUTE_ACT_FLAG = "Auto Compute Actuals"
    COMPLETE_PCT_TYPE = "Percent Complete Type"
    CREATE_DATE = "Added Date"
    CREATE_USER = "Added By"
    CST_DATE = "Primary Constraint Date"
    CST_DATE2 = "Secondary Constraint Date"
    CST_TYPE = "Primary Constraint"
    CST_TYPE2 = "Secondary Constraint"
    DRIVING_PATH_FLAG = "Longest Path"
    DURATION_TYPE = "Duration Type"
    EARLY_START_DATE = "Early Start"
    EARLY_END_DATE = "Early Finish"
    EST_WT = "Est Weight (P6 Professional only)"
    EXPECT_END_DATE = "Expected Finish"
    EXTERNAL_EARLY_START_DATE = "External Early Start"
    EXTERNAL_LATE_END_DATE = "External Late Finish"
    FLOAT_PATH = "Float Path"
    FLOAT_PATH_ORDER = "Float Path Order"
    FREE_FLOAT_HR_CNT = "Free Float"
    GUID = "Global Unique ID"
    LATE_START_DATE = "Late Start"
    LATE_END_DATE = "Late Finish"
    LOCATION_ID = "Activity Location"
    LOCK_PLAN_FLAG = "Lock Remaining"
    PHYS_COMPLETE_PCT = "Complete %"  # Note: There is a blank in the documentation for this field's description
    PRIORITY_TYPE = "Activity Leveling Priority"
    REEND_DATE = "Remaining Early Finish"
    REM_LATE_END_DATE = "Remaining Late Finish"
    REM_LATE_START_DATE = "Remaining Late Start"
    REMAIN_DRTN_HR_CNT = "Remaining Duration"
    REMAIN_EQUIP_QTY = "Remaining Nonlabor Units"
    REMAIN_WORK_QTY = "Remaining Labor Units"
    RESTART_DATE = "Remaining Early Start"
    RESUME_DATE = "Resume Date"
    REV_FDBK_FLAG = "New Feedback"
    REVIEW_END_DATE = "Review Finish (P6 Professional only)"
    REVIEW_TYPE = "Review Status (P6 Professional only)"
    RSRCE_ID = "Primary Resource"
    STATUS_CODE = "Activity Status"
    SUSPEND_DATE = "Suspend Date"
    TARGET_DRTN_HR_CNT = "Planned Duration (P6 EPPM)"
    TARGET_END_DATE = "Planned Finish"
    TARGET_EQUIP_QTY = "Planned Nonlabor Units (P6 EPPM)"
    TARGET_START_DATE = "Planned Start"
    TARGET_WORK_QTY = "Planned Labor Units (P6 EPPM)"
    TASK_TYPE = "Activity Type"
    TEMPL_GUID = "Methodology Global Unique ID"
    TOTAL_FLOAT_HR_CNT = "Total Float"
    UPDATE_DATE = "Last Modified Date"
    UPDATE_USER = "Last Modified By"


@dataclass
class TASK:
    data: Dict[str, str]  # Dictionary to hold the data

    def __post_init__(self):
        # Convert dictionary keys to TaskFields Enum for type checking
        self.data = {TaskFields[k]: v for k, v in self.data.items()}

    def to_dataframe(self) -> pd.DataFrame:
        # Convert the data dictionary to a pandas DataFrame
        return pd.DataFrame([self.data])


# def create_tasks_from_dataframe(df: pd.DataFrame) -> List[Task]:
#     # Convert a pandas DataFrame to a list of Task objects
#     tasks = []
#     for index, row in df.iterrows():
#         tasks.append(Task(row.to_dict()))
#     return tasks


def calculate_completion(task):
    if pd.notnull(task['act_start_date']) and pd.notnull(task['act_end_date']):
        actual_duration = (task['act_end_date'] - task['act_start_date']) / np.timedelta64(1, 'D')
        planned_duration = float(task['target_drtn_hr_cnt']) / 8.0

        # We use a small threshold value (epsilon) to check for division by zero
        epsilon = 1e-10

        if abs(planned_duration) < epsilon or abs(actual_duration) < epsilon:
            return np.nan

        return actual_duration / planned_duration
    return np.nan