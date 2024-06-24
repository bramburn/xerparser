# project.py

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List

import pandas as pd


class ProjectFields(Enum):
    PROJ_ID = "Unique ID"
    PROJ_SHORT_NAME = "Project ID"
    PROJ_URL = "Project Web Site URL"
    FY_START_MONTH_NUM = "Fiscal Year Begins"
    ACCT_ID = "Default Cost Account"
    BASE_TYPE_ID = "Baseline Type"
    CLNDR_ID = "Default Calendar"
    SUM_BASE_PROJ_ID = "Project Baseline"
    TASK_CODE_BASE = "Activity ID Suffix"
    TASK_CODE_STEP = "Activity ID Increment"
    PRIORITY_NUM = "Project Leveling Priority"
    WBS_MAX_SUM_LEVEL = "WBS Max Summarization Level"
    RISK_LEVEL = "Risk Level (P6 Professional only)"
    STRGY_PRIORITY_NUM = "Strategic Priority"
    LAST_CHECKSUM = "Last Checksum"
    CRITICAL_DRTN_HR_CNT = "Critical activities have float less than or equal to"
    DEF_COST_PER_QTY = "Default Price / Unit"
    ALLOW_COMPLETE_FLAG = "Can resources mark activities completed"
    ALLOW_NEG_ACT_FLAG = "Allow Negative Actual Units"
    ACT_PCT_LINK_FLAG = "Link Percent Complete With Actual"
    ACT_THIS_PER_LINK_FLAG = "Link actual to date and actual this period units and costs"
    ADD_ACT_REMAIN_FLAG = "Add Actual To Remain"
    BATCH_SUM_FLAG = "Enable Summarization"
    CHECKOUT_FLAG = "Project Check-out Status"
    CHNG_EFF_CMP_PCT_FLAG = "Resources Edit Percent Complete (P6 Professional only)"
    CONTROL_UPDATES_FLAG = "Status Update Control"
    COST_QTY_RECALC_FLAG = "Cost Qty Recalc Flag"
    DEF_ROLLUP_DATES_FLAG = "Drive Activity Dates Default"
    REM_TARGET_LINK_FLAG = "Link Budget and At Completion"
    RESET_PLANNED_FLAG = "Reset Original to Remaining"
    RSRC_MULTI_ASSIGN_FLAG = "Can assign resource multiple times to activity"
    RSRC_SELF_ADD_FLAG = "Can resources assign selves to activities"
    STEP_COMPLETE_FLAG = "Physical Percent Complete uses Steps Completed"
    SUM_ONLY_FLAG = "Contains Summarized Data Only (P6 Professional only)"
    USE_PROJECT_BASELINE_FLAG = "Use Project Baseline"
    DEF_COMPLETE_PCT_TYPE = "Default Percent Complete Type"
    DEF_DURATION_TYPE = "Default Duration Type"
    DEF_QTY_TYPE = "Default Price Time Units"
    DEF_RATE_TYPE = "Rate Type"
    DEF_TASK_TYPE = "Default Activity Type"
    FCST_START_DATE = "Project Forecast Start"
    LAST_BASELINE_UPDATE_DATE = "Last Update Date"
    LAST_FIN_DATES_ID = "Financial Period"
    LAST_LEVEL_DATE = "Last Leveled Date (P6 EPPM only)"
    LAST_RECALC_DATE = "Last Recalc Date P6 EPPM"
    LAST_SCHEDULE_DATE = "Last Scheduled Date (P6 EPPM only)"
    LAST_TASKSUM_DATE = "Last Summarized Date"
    LOCATION_ID = "Project Location"
    NAME_SEP_CHAR = "Code Separator"
    ORIG_PROJ_ID = "Original Project"
    PLAN_END_DATE = "Must Finish By"
    PLAN_START_DATE = "Planned Start"
    PX_ENABLE_PUBLICATION_FLAG = "Enable Publication (P6 Professional only)"
    PX_LAST_UPDATE_DATE = "Last time Publish Project was run on this project (P6 Professional only)"
    PX_PRIORITY = "Publication Priority (P6 EPPM only)"
    SCD_END_DATE = "Schedule Finish"
    SOURCE_PROJ_ID = "Source Project"
    SUM_ASSIGN_LEVEL = "Summarization Level"
    TASK_CODE_PREFIX = "Activity ID Prefix"
    TASK_CODE_PREFIX_FLAG = "Activity ID based on selected activity"
    TS_RSRC_VS_INACT_ACTV_FLAG = "Resource can view activities from an inactive project (P6 Professional only)"
    WEB_LOCAL_ROOT_PATH = "Web Site Root Directory"
    GUID = "Global Unique ID"
    ADD_BY_NAME = "Added By"
    ADD_DATE = "Date Added"
    CHECKOUT_DATE = "Date Checked Out"
    CHECKOUT_USER_ID = "Checked Out By"

    CRITICAL_PATH_TYPE = "Critical Path Type (P6 Professional only)"

    INTG_PROJ_TYPE = "Integrated Project (P6 Professional only)"



@dataclass
class Project:
    data: Dict[ProjectFields, str]  # Dictionary to hold the data

    def __post_init__(self):
        # Convert dictionary keys to ProjectFields Enum for type checking
        self.data = {ProjectFields[k]: v for k, v in self.data.items()}

    def to_dataframe(self) -> pd.DataFrame:
        # Convert the data dictionary to a pandas DataFrame
        return pd.DataFrame([self.data])


def create_projects_from_dataframe(df: pd.DataFrame) -> List[Project]:
    # Convert a pandas DataFrame to a list of Project objects
    projects = []
    for index, row in df.iterrows():
        projects.append(Project(row.to_dict()))
    return projects