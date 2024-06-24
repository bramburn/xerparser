# xerparser
# task.py

from datetime import datetime
from enum import Enum
from functools import cached_property

import pandas as pd

from xerparser.scripts.decorators import rounded
from xerparser.src.validators import (
    date_format,
    float_or_zero,
    optional_date,
    optional_float,
    optional_int,
    optional_str,
)


class TASK:
    """
    A class to represent a schedule activity.
    """

    class ConstraintType(Enum):
        """Map codes used for constraint types to readable descriptions"""
        # ... (enum definitions)

    class PercentType(Enum):
        """Map codes used for percent types to readable descriptions"""
        # ... (enum definitions)

    class TaskStatus(Enum):
        """Map codes used for Task status to readable descriptions"""
        # ... (enum definitions)

    class TaskType(Enum):
        """Map codes used for Task types to readable descriptions"""
        # ... (enum definitions)

    def __init__(self, task_df: pd.DataFrame) -> None:
        self.task_df = task_df
        self.task_dict = self._process_task_data(task_df)

    def _process_task_data(self, task_df: pd.DataFrame) -> dict[str, "TASK"]:
        task_dict = {}
        for _, row in task_df.iterrows():
            task = {
                "uid": row['task_id'],
                "proj_id": row['proj_id'],
                "wbs_id": row['wbs_id'],
                "clndr_id": row['clndr_id'],
                "act_end_date": optional_date(row['act_end_date']),
                "act_equip_qty": float_or_zero(row['act_equip_qty']),
                "act_start_date": optional_date(row['act_start_date']),
                "act_this_per_equip_qty": float_or_zero(row['act_this_per_equip_qty']),
                "act_this_per_work_qty": float_or_zero(row['act_this_per_work_qty']),
                "act_work_qty": float_or_zero(row['act_work_qty']),
                "auto_compute_act_flag": row['auto_compute_act_flag'] == 'Y',
                "complete_pct_type": row['complete_pct_type'],
                "create_date": datetime.strptime(row['create_date'], date_format),
                "create_user": row['create_user'],
                "cstr_date": optional_date(row['cstr_date']),
                "cstr_date2": optional_date(row['cstr_date2']),
                "cstr_type": optional_str(row['cstr_type']),
                "cstr_type2": optional_str(row['cstr_type2']),
                "driving_path_flag": row['driving_path_flag'] == 'Y',
                "duration_type": row['duration_type'],
                "early_end_date": optional_date(row['early_end_date']),
                "early_start_date": optional_date(row['early_start_date']),
                "est_wt": optional_float(row['est_wt']),
                "expect_end_date": optional_date(row['expect_end_date']),
                "external_early_start_date": optional_date(row['external_early_start_date']),
                "external_late_end_date": optional_date(row['external_late_end_date']),
                "float_path": optional_int(row['float_path']),
                "float_path_order": optional_int(row['float_path_order']),
                "free_float_hr_cnt": optional_float(row['free_float_hr_cnt']),
                "guid": row['guid'],
                "late_end_date": optional_date(row['late_end_date']),
                "late_start_date": optional_date(row['late_start_date']),
                "location_id": optional_str(row['location_id']),
                "lock_plan_flag": row['lock_plan_flag'] == 'Y',
                "phys_complete_pct": row['phys_complete_pct'],
                "priority_type": optional_str(row['priority_type']),
                "reend_date": optional_date(row['reend_date']),
                "rem_late_end_date": optional_date(row['rem_late_end_date']),
                "rem_late_start_date": optional_date(row['rem_late_start_date']),
                "remain_drtn_hr_cnt": row['remain_drtn_hr_cnt'],
                "remain_equip_qty": float_or_zero(row['remain_equip_qty']),
                "remain_work_qty": float_or_zero(row['remain_work_qty']),
                "restart_date": optional_date(row['restart_date']),
                "resume_date": optional_date(row['resume_date']),
                "rev_fdbk_flag": row['rev_fdbk_flag'] == 'Y',
                "review_end_date": optional_date(row['review_end_date']),
                "review_type": optional_str(row['review_type']),
                "rsrc_id": optional_str(row['rsrc_id']),
                "status_code": row['status_code'],
                "suspend_date": optional_date(row['suspend_date']),
                "target_drtn_hr_cnt": row['target_drtn_hr_cnt'],
                "target_end_date": datetime.strptime(row['target_end_date'], date_format),
                "target_equip_qty": float_or_zero(row['target_equip_qty']),
                "target_start_date": datetime.strptime(row['target_start_date'], date_format),
                "target_work_qty": float_or_zero(row['target_work_qty']),
                "task_code": row['task_code'],
                "task_name": row['task_name'],
                "task_type": TASK.TaskType[row['task_type']],
                "tmpl_guid": row['tmpl_guid'],
                "total_float_hr_cnt": optional_float(row['total_float_hr_cnt']),
                "update_date": datetime.strptime(row['update_date'], date_format),
                "update_user": row['update_user']
            }
            task_dict[task["uid"]] = TASK(task)
        return task_dict

    def get_task(self, task_id: str) -> dict:
        return self.task_dict[task_id]

    def __eq__(self, __o: "TASK") -> bool:
        return self.task_dict[self.task_df.iloc[0]["task_id"]]["task_code"] == \
            self.task_dict[__o.task_df.iloc[0]["task_id"]]["task_code"]

    def __lt__(self, __o: "TASK") -> bool:
        if self == __o:
            if self.start == __o.start:
                return self.finish < __o.finish
            return self.start < __o.start
        return self.task_dict[self.task_df.iloc[0]["task_id"]]["task_code"] < \
            self.task_dict[__o.task_df.iloc[0]["task_id"]]["task_code"]

    def __gt__(self, __o: "TASK") -> bool:
        if self == __o:
            if self.start == __o.start:
                return self.finish > __o.finish
            return self.start > __o.start
        return self.task_dict[self.task_df.iloc[0]["task_id"]]["task_code"] > \
            self.task_dict[__o.task_df.iloc[0]["task_id"]]["task_code"]

    def __hash__(self) -> int:
        return hash(self.task_dict[self.task_df.iloc[0]["task_id"]]["task_code"])

    def __str__(self) -> str:
        return f"{self.task_dict[self.task_df.iloc[0]['task_code']]['task_code']} - {self.task_dict[self.task_df.iloc[0]['task_name']]}"

    @property
    @rounded()
    def actual_cost(self) -> float:
        # This method is not implemented since the relationships are removed
        return 0.0

    @property
    @rounded()
    def at_completion_cost(self) -> float:
        # This method is not implemented since the relationships are removed
        return 0.0

    @property
    @rounded()
    def budgeted_cost(self) -> float:
        # This method is not implemented since the relationships are removed
        return 0.0

    @property
    def constraints(self) -> dict:
        return {
            "prime": {
                "type": TASK.ConstraintType[self.task_dict[self.task_df.iloc[0]["task_id"]]["cstr_type"]] if
                self.task_dict[self.task_df.iloc[0]["task_id"]]["cstr_type"] else None,
                "date": self.task_dict[self.task_df.iloc[0]["task_id"]]["cstr_date"],
            },
            "second": {
                "type": TASK.ConstraintType[self.task_dict[self.task_df.iloc[0]["task_id"]]["cstr_type2"]] if
                self.task_dict[self.task_df.iloc[0]["task_id"]]["cstr_type2"] else None,
                "date": self.task_dict[self.task_df.iloc[0]["task_id"]]["cstr_date2"],
            },
        }

    @property
    def duration(self) -> int:
        """
        Returns remaining duration if task is not started;
        otherwise, returns original duration.
        """
        if self.status.is_not_started:
            return self.remaining_duration
        return self.original_duration

    @property
    def finish(self) -> datetime:
        """Calculated activity finish date (Actual Finish or Early Finish)"""
        if self.task_dict[self.task_df.iloc[0]["task_id"]]["act_end_date"]:
            return self.task_dict[self.task_df.iloc[0]["task_id"]]["act_end_date"]
        if self.task_dict[self.task_df.iloc[0]["task_id"]]["reend_date"]:
            return self.task_dict[self.task_df.iloc[0]["task_id"]]["reend_date"]
        if self.task_dict[self.task_df.iloc[0]["task_id"]]["early_end_date"]:
            return self.task_dict[self.task_df.iloc[0]["task_id"]]["early_end_date"]
        raise ValueError(f"Could not find finish date for task {self.task_dict[self.task_df.iloc[0]['task_code']]}")

    @property
    def free_float(self) -> int | None:
        if not self.task_dict[self.task_df.iloc[0]["task_id"]]["free_float_hr_cnt"]:
            return None

        return int(self.task_dict[self.task_df.iloc[0]["task_id"]]["free_float_hr_cnt"] / 8)

    @property
    def is_critical(self) -> bool:
        return self.task_dict[self.task_df.iloc[0]["task_id"]]["total_float_hr_cnt"] is not None and \
            self.task_dict[self.task_df.iloc[0]["task_id"]]["total_float_hr_cnt"] <= 0

    @property
    def original_duration(self) -> int:
        """Original Duration in Days"""
        return int(self.task_dict[self.task_df.iloc[0]["task_id"]]["target_drtn_hr_cnt"] / 8)

    @cached_property
    @rounded(ndigits=4)
    def percent_complete(self) -> float:
        if self.percent_type is TASK.PercentType.CP_Phys:
            return self.task_dict[self.task_df.iloc[0]["task_id"]]["phys_complete_pct"] / 100

        if self.percent_type is TASK.PercentType.CP_Drtn:
            if self.task_dict[self.task_df.iloc[0]["task_id"]][
                "remain_drtn_hr_cnt"] is None or self.status.is_completed:
                return 1.0
            if self.status.is_not_started or self.original_duration == 0:
                return 0.0
            if self.task_dict[self.task_df.iloc[0]["task_id"]]["remain_drtn_hr_cnt"] >= \
                    self.task_dict[self.task_df.iloc[0]["task_id"]]["target_drtn_hr_cnt"]:
                return 0.0

            return 1 - self.task_dict[self.task_df.iloc[0]["task_id"]]["remain_drtn_hr_cnt"] / \
                self.task_dict[self.task_df.iloc[0]["task_id"]]["target_drtn_hr_cnt"]

        if self.percent_type is TASK.PercentType.CP_Units:
            target_units = self.task_dict[self.task_df.iloc[0]["task_id"]]["target_work_qty"] + \
                           self.task_dict[self.task_df.iloc[0]["task_id"]]["target_equip_qty"]
            if target_units == 0:
                return 0.0
            actual_units = self.task_dict[self.task_df.iloc[0]["task_id"]]["act_work_qty"] + \
                           self.task_dict[self.task_df.iloc[0]["task_id"]]["act_equip_qty"]
            return 1 - actual_units / target_units

        raise ValueError(
            f"Could not calculate percent complete for task {self.task_dict[self.task_df.iloc[0]['task_code']]}"
        )

    @property
    def percent_type(self) -> PercentType:
        return TASK.PercentType[self.task_dict[self.task_df.iloc[0]["task_id"]]["complete_pct_type"]]

    @property
    @rounded()
    def remaining_cost(self) -> float:
        # This method is not implemented since the relationships are removed
        return 0.0

    @property
    def remaining_duration(self) -> int:
        if self.task_dict[self.task_df.iloc[0]["task_id"]]["remain_drtn_hr_cnt"] is None:
            return 0
        return int(self.task_dict[self.task_df.iloc[0]["task_id"]]["remain_drtn_hr_cnt"] / 8)

    def rem_hours_per_day(self, late_dates=False) -> dict[datetime, float]:
        # This method is not implemented since the relationships are removed
        return {}

    @property
    def start(self) -> datetime:
        """Calculated activity start date (Actual Start or Early Start)"""
        if self.task_dict[self.task_df.iloc[0]["task_id"]]["act_start_date"]:
            return self.task_dict[self.task_df.iloc[0]["task_id"]]["act_start_date"]
        if self.task_dict[self.task_df.iloc[0]["task_id"]]["early_start_date"]:
            return self.task_dict[self.task_df.iloc[0]["task_id"]]["early_start_date"]
        raise ValueError(f"Could not find start date for task {self.task_dict[self.task_df.iloc[0]['task_code']]} ")

    @property
    @rounded()
    def this_period_cost(self) -> float:
        total_this_period_cost = 0.0
        for task_id, task_data in self.task_dict.items():
            total_this_period_cost += task_data["act_this_per_cost"]
        return total_this_period_cost

    @property
    def total_float(self) -> int | None:
        if self.task_dict[self.task_df.iloc[0]["task_id"]]["total_float_hr_cnt"] is None:
            return
        return int(self.task_dict[self.task_df.iloc[0]["task_id"]]["total_float_hr_cnt"] / 8)


def _process_task_data(task_df: pd.DataFrame) -> dict[str, TASK]:
    task_dict = {}
    for _, row in task_df.iterrows():
        task = TASK(task_df)
        task_dict[task.uid] = task
    return task_dict
