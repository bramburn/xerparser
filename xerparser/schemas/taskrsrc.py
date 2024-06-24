# taskrsrc.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd

from xerparser.src.validators import optional_date, optional_float, optional_int, optional_str

@dataclass(frozen=True)
class TASKRSRC:
    """
    A class to represent an Activity Resource Assignment.
    """
    taskrsrc_id: str
    """Unique ID"""
    task_id: str
    """Activity Name"""
    rsrc_id: str
    """Resource ID Name"""
    proj_id: str
    """Project"""
    acct_id: str
    """Cost Account"""
    act_start_date: Optional[datetime]
    """Actual Start"""
    act_end_date: Optional[datetime]
    """Actual Finish"""
    act_reg_qty: Optional[float]
    """Actual Regular Units"""
    act_ot_qty: Optional[float]
    """Actual Overtime Units"""
    act_reg_cost: Optional[float]
    """Actual Regular Cost"""
    act_ot_cost: Optional[float]
    """Actual Overtime Cost"""
    act_this_per_cost: Optional[float]
    """Actual This Period Cost"""
    act_this_per_qty: Optional[float]
    """Actual This Period Units"""
    actual_crv: Optional[str]
    """Actual Units Profile"""
    cost_per_qty: Optional[float]
    """Price / Unit"""
    cost_per_qty_source_type: Optional[str]
    """Rate Source"""
    cost_qty_link_flag: bool
    """Calculate costs from units"""
    create_date: datetime
    """Assigned Date"""
    create_user: str
    """Assigned by"""
    curv_id: Optional[str]
    """Curve"""
    guid: str
    """Global Unique ID"""
    ot_factor: Optional[float]
    """Overtime Factor"""
    pend_act_ot_qty: Optional[float]
    """Pend Actual Overtime Units (P6 Professional only)"""
    pend_act_reg_qty: Optional[float]
    """Pend Actual Regular Units (P6 Professional only)"""
    pend_complete_pct: Optional[float]
    """Pend % Complete (P6 Professional only)"""
    pend_remain_qty: Optional[float]
    """Pend Remaining Units (P6 Professional only)"""
    prior_ts_act_of_qty: Optional[float]
    """Prior Timesheet Actual Overtime Units (P6 Professional only)"""
    prior_ts_act_reg_qty: Optional[float]
    """Prior Timesheet Actual Regular Units (P6 Professional only)"""
    rate_type: Optional[str]
    """Rate Type"""
    reend_date: Optional[datetime]
    """Remaining Early Finish"""
    relag_drtn_hr_cnt: Optional[float]
    """Remaining Lag"""
    rem_late_end_date: Optional[datetime]
    """Remaining Late Finish"""
    rem_late_start_date: Optional[datetime]
    """Remaining Late Start"""
    remain_cost: Optional[float]
    """Remaining Early Cost"""
    remain_crv: Optional[str]
    """Remaining Units Profile"""
    remain_qty: Optional[float]
    """Remaining Early Units"""
    remain_qty_per_hr: Optional[float]
    """Remaining Units / Time"""
    restart_date: Optional[datetime]
    """Remaining Early Start"""
    role_id: Optional[str]
    """Role"""
    rollup_dates_flag: bool
    """Drive Activity Dates"""
    skill_level: Optional[str]
    """Proficiency"""
    target_cost: Optional[float]
    """Budgeted/Planned Cost"""
    target_crv: Optional[str]
    """Planned Units Profile"""
    target_end_date: Optional[datetime]
    """Planned Finish"""
    target_lag_drtn_hr_cnt: Optional[float]
    """Original Lag"""
    target_qty: Optional[float]
    """Budgeted/Planned Units"""
    target_qty_per_hr: Optional[float]
    """Budgeted/Planned Units / Time"""
    target_start_date: Optional[datetime]
    """Planned Start"""
    ts_pend_act_end_flag: bool
    """Pending Actual End Date Flag"""
    wbs_id: str
    """EPS/WBS"""

    def __eq__(self, __o: "TASKRSRC") -> bool:
        return self.taskrsrc_id == __o.taskrsrc_id

    def __hash__(self) -> int:
        return hash(self.taskrsrc_id)

def _process_taskrsrc_data(taskrsrc_df: pd.DataFrame) -> dict[str, TASKRSRC]:
    taskrsrc_dict = {}
    for _, row in taskrsrc_df.iterrows():
        taskrsrc = TASKRSRC(
            taskrsrc_id=row["taskrsrc_id"],
            task_id=row["task_id"],
            rsrc_id=row["rsrc_id"],
            proj_id=row["proj_id"],
            acct_id=row["acct_id"],
            act_start_date=optional_date(row["act_start_date"]),
            act_end_date=optional_date(row["act_end_date"]),
            act_reg_qty=optional_float(row["act_reg_qty"]),
            act_ot_qty=optional_float(row["act_ot_qty"]),
            act_reg_cost=optional_float(row["act_reg_cost"]),
            act_ot_cost=optional_float(row["act_ot_cost"]),
            act_this_per_cost=optional_float(row["act_this_per_cost"]),
            act_this_per_qty=optional_float(row["act_this_per_qty"]),
            actual_crv=optional_str(row["actual_crv"]),
            cost_per_qty=optional_float(row["cost_per_qty"]),
            cost_per_qty_source_type=optional_str(row["cost_per_qty_source_type"]),
            cost_qty_link_flag=row["cost_qty_link_flag"] == 'Y',
            create_date=pd.to_datetime(row["create_date"]),
            create_user=row["create_user"],
            curv_id=optional_str(row["curv_id"]),
            guid=row["guid"],
            ot_factor=optional_float(row["ot_factor"]),
            pend_act_ot_qty=optional_float(row["pend_act_ot_qty"]),
            pend_act_reg_qty=optional_float(row["pend_act_reg_qty"]),
            pend_complete_pct=optional_float(row["pend_complete_pct"]),
            pend_remain_qty=optional_float(row["pend_remain_qty"]),
            prior_ts_act_of_qty=optional_float(row["prior_ts_act_of_qty"]),
            prior_ts_act_reg_qty=optional_float(row["prior_ts_act_reg_qty"]),
            rate_type=optional_str(row["rate_type"]),
            reend_date=optional_date(row["reend_date"]),
            relag_drtn_hr_cnt=optional_float(row["relag_drtn_hr_cnt"]),
            rem_late_end_date=optional_date(row["rem_late_end_date"]),
            rem_late_start_date=optional_date(row["rem_late_start_date"]),
            remain_cost=optional_float(row["remain_cost"]),
            remain_crv=optional_str(row["remain_crv"]),
            remain_qty=optional_float(row["remain_qty"]),
            remain_qty_per_hr=optional_float(row["remain_qty_per_hr"]),
            restart_date=optional_date(row["restart_date"]),
            role_id=optional_str(row["role_id"]),
            rollup_dates_flag=row["rollup_dates_flag"] == 'Y',
            skill_level=optional_str(row["skill_level"]),
            target_cost=optional_float(row["target_cost"]),
            target_crv=optional_str(row["target_crv"]),
            target_end_date=optional_date(row["target_end_date"]),
            target_lag_drtn_hr_cnt=optional_float(row["target_lag_drtn_hr_cnt"]),
            target_qty=optional_float(row["target_qty"]),
            target_qty_per_hr=optional_float(row["target_qty_per_hr"]),
            target_start_date=optional_date(row["target_start_date"]),
            ts_pend_act_end_flag=row["ts_pend_act_end_flag"] == 'Y',
            wbs_id=row["TASKRSRC.TASK|wbs_id"]
        )
        taskrsrc_dict[taskrsrc.taskrsrc_id] = taskrsrc
    return taskrsrc_dict