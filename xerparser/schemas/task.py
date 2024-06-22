# xerparser
# task.py

from datetime import datetime
from enum import Enum
from functools import cached_property
from typing import Any, Optional

import pandas as pd

from xerparser.schemas.actvcode import ACTVCODE
from xerparser.schemas.actvtype import ACTVTYPE
from xerparser.schemas.calendars import CALENDAR
from xerparser.schemas.projwbs import PROJWBS
from xerparser.schemas.taskfin import TASKFIN
from xerparser.schemas.taskmemo import TASKMEMO
from xerparser.schemas.taskrsrc import TASKRSRC
from xerparser.schemas.udftype import UDFTYPE
from xerparser.scripts.dates import clean_date
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

        CS_ALAP = "As Late as Possible"
        CS_MEO = "Finish On"
        CS_MEOA = "Finish on or After"
        CS_MEOB = "Finish on or Before"
        CS_MANDFIN = "Mandatory Finish"
        CS_MANDSTART = "Mandatory Start"
        CS_MSO = "Start On"
        CS_MSOA = "Start On or After"
        CS_MSOB = "Start On or Before"

    class PercentType(Enum):
        """Map codes used for percent types to readable descriptions"""

        CP_Phys = "Physical"
        CP_Drtn = "Duration"
        CP_Units = "Unit"

    class TaskStatus(Enum):
        """Map codes used for Task status to readable descriptions"""

        TK_NotStart = "Not Started"
        TK_Active = "In Progress"
        TK_Complete = "Complete"

        @property
        def is_not_started(self) -> bool:
            return self is self.TK_NotStart

        @property
        def is_in_progress(self) -> bool:
            return self is self.TK_Active

        @property
        def is_completed(self) -> bool:
            return self is self.TK_Complete

        @property
        def is_open(self) -> bool:
            return self is not self.TK_Complete

    class TaskType(Enum):
        """Map codes used for Task types to readable descriptions"""

        TT_Mile = "Start Milestone"
        TT_FinMile = "Finish Milestone"
        TT_LOE = "Level of Effort"
        TT_Task = "Task Dependent"
        TT_Rsrc = "Resource Dependent"
        TT_WBS = "WBS Summary"

        @property
        def is_milestone(self) -> bool:
            return self is self.TT_FinMile or self is self.TT_Mile

        @property
        def is_loe(self) -> bool:
            return self is self.TT_LOE

        @property
        def is_task(self) -> bool:
            return self is self.TT_Task

        @property
        def is_wbs(self) -> bool:
            return self is self.TT_WBS

    def __init__(self, row: pd.Series, calendar: CALENDAR, wbs: PROJWBS) -> None:
        self.uid: str = row['task_id']
        self.proj_id: str = row['proj_id']
        self.wbs_id: str = row['wbs_id']
        self.clndr_id: str = row['clndr_id']
        self.act_end_date: Optional[datetime] = optional_date(row['act_end_date'])
        self.act_equip_qty: float = float_or_zero(row['act_equip_qty'])
        self.act_start_date: Optional[datetime] = optional_date(row['act_start_date'])
        self.act_this_per_equip_qty: float = float_or_zero(row['act_this_per_equip_qty'])
        self.act_this_per_work_qty: float = float_or_zero(row['act_this_per_work_qty'])
        self.act_work_qty: float = float_or_zero(row['act_work_qty'])
        self.auto_compute_act_flag: bool = row['auto_compute_act_flag'] == 'Y'
        self.complete_pct_type: str = row['complete_pct_type']
        self.create_date: datetime = datetime.strptime(row['create_date'], date_format)
        self.create_user: str = row['create_user']
        self.cstr_date: Optional[datetime] = optional_date(row['cstr_date'])
        self.cstr_date2: Optional[datetime] = optional_date(row['cstr_date2'])
        self.cstr_type: Optional[str] = optional_str(row['cstr_type'])
        self.cstr_type2: Optional[str] = optional_str(row['cstr_type2'])
        self.driving_path_flag: bool = row['driving_path_flag'] == 'Y'
        self.duration_type: str = row['duration_type']
        self.early_end_date: Optional[datetime] = optional_date(row['early_end_date'])
        self.early_start_date: Optional[datetime] = optional_date(row['early_start_date'])
        self.est_wt: Optional[float] = optional_float(row['est_wt'])
        self.expect_end_date: Optional[datetime] = optional_date(row['expect_end_date'])
        self.external_early_start_date: Optional[datetime] = optional_date(row['external_early_start_date'])
        self.external_late_end_date: Optional[datetime] = optional_date(row['external_late_end_date'])
        self.float_path: Optional[int] = optional_int(row['float_path'])
        self.float_path_order: Optional[int] = optional_int(row['float_path_order'])
        self.free_float_hr_cnt: Optional[float] = optional_float(row['free_float_hr_cnt'])
        self.guid: str = row['guid']
        self.late_end_date: Optional[datetime] = optional_date(row['late_end_date'])
        self.late_start_date: Optional[datetime] = optional_date(row['late_start_date'])
        self.location_id: Optional[str] = optional_str(row['location_id'])
        self.lock_plan_flag: bool = row['lock_plan_flag'] == 'Y'
        self.phys_complete_pct: float = row['phys_complete_pct']
        self.priority_type: Optional[str] = optional_str(row['priority_type'])
        self.reend_date: Optional[datetime] = optional_date(row['reend_date'])
        self.rem_late_end_date: Optional[datetime] = optional_date(row['rem_late_end_date'])
        self.rem_late_start_date: Optional[datetime] = optional_date(row['rem_late_start_date'])
        self.remain_drtn_hr_cnt: float = row['remain_drtn_hr_cnt']
        self.remain_equip_qty: float = float_or_zero(row['remain_equip_qty'])
        self.remain_work_qty: float = float_or_zero(row['remain_work_qty'])
        self.restart_date: Optional[datetime] = optional_date(row['restart_date'])
        self.resume_date: Optional[datetime] = optional_date(row['resume_date'])
        self.rev_fdbk_flag: bool = row['rev_fdbk_flag'] == 'Y'
        self.review_end_date: Optional[datetime] = optional_date(row['review_end_date'])
        self.review_type: Optional[str] = optional_str(row['review_type'])
        self.rsrc_id: Optional[str] = optional_str(row['rsrc_id'])
        self.status_code: str = row['status_code']
        self.suspend_date: Optional[datetime] = optional_date(row['suspend_date'])
        self.target_drtn_hr_cnt: float = row['target_drtn_hr_cnt']
        self.target_end_date: datetime = datetime.strptime(row['target_end_date'], date_format)
        self.target_equip_qty: float = float_or_zero(row['target_equip_qty'])
        self.target_start_date: datetime = datetime.strptime(row['target_start_date'], date_format)
        self.target_work_qty: float = float_or_zero(row['target_work_qty'])
        self.task_code: str = row['task_code']
        self.task_name: str = row['task_name']
        self.task_type: TASK.TaskType = TASK.TaskType[row['task_type']]
        self.tmpl_guid: str = row['tmpl_guid']
        self.total_float_hr_cnt: Optional[float] = optional_float(row['total_float_hr_cnt'])
        self.update_date: datetime = datetime.strptime(row['update_date'], date_format)
        self.update_user: str = row['update_user']

        self.activity_codes: dict[ACTVTYPE, ACTVCODE] = {}
        self.user_defined_fields: dict[UDFTYPE, Any] = {}
        self.memos: list[TASKMEMO] = []
        self.resources: dict[str, TASKRSRC] = {}
        self.predecessors: list["LinkToTask"] = []
        self.successors: list["LinkToTask"] = []
        self.periods: list[TASKFIN] = []

    def __eq__(self, __o: "TASK") -> bool:
        return self.task_code == __o.task_code

    def __lt__(self, __o: "TASK") -> bool:
        if self == __o:
            if self.start == __o.start:
                return self.finish < __o.finish
            return self.start < __o.start
        return self.task_code < __o.task_code

    def __gt__(self, __o: "TASK") -> bool:
        if self == __o:
            if self.start == __o.start:
                return self.finish > __o.finish
            return self.start > __o.start
        return self.task_code > __o.task_code

    def __hash__(self) -> int:
        return hash(self.task_code)

    def __str__(self) -> str:
        return f"{self.task_code} - {self.name}"

    @property
    @rounded()
    def actual_cost(self) -> float:
        return sum(res.act_total_cost for res in self.resources.values())

    @property
    @rounded()
    def at_completion_cost(self) -> float:
        return sum(res.at_completion_cost for res in self.resources.values())

    @property
    @rounded()
    def budgeted_cost(self) -> float:
        return sum(res.target_cost for res in self.resources.values())

    @property
    def constraints(self) -> dict:
        return {
            "prime": {
                "type": TASK.ConstraintType[self.cstr_type] if self.cstr_type else None,
                "date": self.cstr_date,
            },
            "second": {
                "type": TASK.ConstraintType[self.cstr_type2]
                if self.cstr_type2
                else None,
                "date": self.cstr_date2,
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
        if self.act_end_date:
            return self.act_end_date
        if self.reend_date:
            return self.reend_date
        if self.early_end_date:
            return self.early_end_date
        raise ValueError(f"Could not find finish date for task {self.task_code}")

    @property
    def free_float(self) -> int | None:
        if not self.free_float_hr_cnt:
            return None

        return int(self.free_float_hr_cnt / 8)

    @property
    def is_critical(self) -> bool:
        return self.total_float_hr_cnt is not None and self.total_float_hr_cnt <= 0

    @property
    def original_duration(self) -> int:
        """Original Duration in Days"""
        return int(self.target_drtn_hr_cnt / 8)

    @cached_property
    @rounded(ndigits=4)
    def percent_complete(self) -> float:
        if self.percent_type is TASK.PercentType.CP_Phys:
            return self.phys_complete_pct / 100

        if self.percent_type is TASK.PercentType.CP_Drtn:
            if self.remain_drtn_hr_cnt is None or self.status.is_completed:
                return 1.0
            if self.status.is_not_started or self.original_duration == 0:
                return 0.0
            if self.remain_drtn_hr_cnt >= self.target_drtn_hr_cnt:
                return 0.0

            return 1 - self.remain_drtn_hr_cnt / self.target_drtn_hr_cnt

        if self.percent_type is TASK.PercentType.CP_Units:
            target_units = self.target_work_qty + self.target_equip_qty
            if target_units == 0:
                return 0.0
            actual_units = self.act_work_qty + self.act_equip_qty
            return 1 - actual_units / target_units

        raise ValueError(
            f"Could not calculate percent complete for task {self.task_code}"
        )

    @property
    def percent_type(self) -> PercentType:
        return TASK.PercentType[self.complete_pct_type]

    @property
    @rounded()
    def remaining_cost(self) -> float:
        return sum(res.remain_cost for res in self.resources.values())

    @property
    def remaining_duration(self) -> int:
        if self.remain_drtn_hr_cnt is None:
            return 0
        return int(self.remain_drtn_hr_cnt / 8)

    def rem_hours_per_day(self, late_dates=False) -> dict[datetime, float]:
        """
        Calculate the remaining workhours per day for a task.
        Will only return valid workdays in a list of tuples containing
        the date and workhour values.
        This is useful for calculating projections like cash flow.

        P6 calculates remaining duration based on hours rather than days,
        So the start and/or finish date for an activity may be a partial day.

        Returns:
            dict[datetime, float]: date and workhour pairs
        """
        if self.calendar is None:
            raise ValueError(f"Calendar is not set for task {self.task_code}")

        if self.wbs is None:
            raise ValueError(f"WBS is not set for task {self.task_code}")

        if self.remain_drtn_hr_cnt == 0:
            return {}

        if self.status.is_completed or not self.restart_date:
            return {}

        if late_dates and (not self.rem_late_end_date or not self.rem_late_start_date):
            return {}

        if late_dates and self.rem_late_end_date and self.rem_late_start_date:
            start_date = self.rem_late_start_date
            end_date = self.rem_late_end_date
        else:
            start_date = self.restart_date
            end_date = self.finish

        # edge case that start and end dates are equal
        if start_date.date() == end_date.date():
            work_hrs = self.calendar._calc_work_hours(
                start_date, start_date.time(), end_date.time()
            )
            return {clean_date(start_date): round(work_hrs, 3)}

        # Get a list of all workdays between the start and end dates
        date_range = list(self.calendar.iter_workdays(start_date, end_date))

        # edge cases that only 1 valid workday between start date and end date these
        # may never actually occur since the dates are pulled directly from the schedule
        # did not find any case where these occur in testing, but leaving anyway
        if len(date_range) == 1 and end_date.date() > start_date.date():
            if start_date.date() == date_range[0].date():
                work_day = self.calendar._get_workday(start_date)
                work_hrs = self.calendar._calc_work_hours(
                    start_date, start_date.time(), work_day.finish
                )
                return {clean_date(start_date): round(work_hrs, 3)}

            if end_date.date() == date_range[0].date():
                work_day = self.calendar._get_workday(end_date)
                work_hrs = self.calendar._calc_work_hours(
                    end_date, work_day.start, end_date.time()
                )
                return {clean_date(end_date): round(work_hrs, 3)}

            work_day = self.calendar._get_workday(date_range[0])
            return {clean_date(date_range[0]): round(work_day.hours, 3)}

        # cases were multiple valid workdays between start and end date
        # initialize hours with start date
        rem_hrs = {
            clean_date(start_date): round(
                self.calendar._calc_work_hours(
                    date_to_calc=start_date,
                    start_time=start_date.time(),
                    end_time=self.calendar._get_workday(start_date).finish,
                ),
                3,
            ),
        }

        # loop through 2nd to 2nd to last day in date range
        # these would be a full workday
        for dt in date_range[1: len(date_range) - 1]:
            if not self.calendar.is_workday(dt):
                continue
            if wd := self.calendar._get_workday(dt):
                rem_hrs[dt] = round(wd.hours, 3)

        # calculate work hours for the last day
        rem_hrs[clean_date(end_date)] = round(
            self.calendar._calc_work_hours(
                date_to_calc=end_date,
                start_time=self.calendar._get_workday(end_date).start,
                end_time=end_date.time(),
            ),
            3,
        )

        return rem_hrs

    @property
    def start(self) -> datetime:
        """Calculated activity start date (Actual Start or Early Start)"""
        if self.act_start_date:
            return self.act_start_date
        if self.early_start_date:
            return self.early_start_date
        raise ValueError(f"Could not find start date for task {self.task_code}")

    @property
    @rounded()
    def this_period_cost(self) -> float:
        return sum(res.act_this_per_cost for res in self.resources.values())

    @property
    def total_float(self) -> int | None:
        if self.total_float_hr_cnt is None:
            return
        return int(self.total_float_hr_cnt / 8)

    def _valid_projwbs(self, value: PROJWBS) -> PROJWBS:
        if not isinstance(value, PROJWBS):
            raise TypeError(f"Expected <class PROJWBS>; got {type(value)}")
        if value.uid != self.wbs_id:
            raise ValueError(
                f"WBS unique id {value.uid} does not match wbs_id {self.wbs_id}"
            )
        return value

    def set_calendar(self, calendars: dict[str, CALENDAR]) -> None:
        if self.clndr_id in calendars:
            self.calendar = calendars[self.clndr_id]
        else:
            self.calendar = None

    def set_wbs(self, wbs_nodes: dict[str, PROJWBS]) -> None:
        if self.wbs_id in wbs_nodes:
            self.wbs = wbs_nodes[self.wbs_id]
        else:
            self.wbs = None


class LinkToTask:
    """
    A class to represent a logic tie to another activity
    """

    def __init__(self, task: TASK, link: str, lag_days: int) -> None:
        if link.upper() not in ("FF", "FS", "SF", "SS"):
            raise AttributeError(
                f"link attribute must have a value FF, FS, SF, or SS; got {link}"
            )
        self.task: TASK = task
        self.link: str = link
        self.lag: int = lag_days

    def __eq__(self, __o: "LinkToTask") -> bool:
        return all((self.task == __o.task, self.link == __o.link))

    def __hash__(self) -> int:
        return hash((self.task, self.link))
