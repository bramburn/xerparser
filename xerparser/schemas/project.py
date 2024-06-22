# xerparser
# project.py
# xerparser
# project.py

from collections import Counter
from datetime import datetime
from functools import cached_property
from statistics import mean
from typing import Any, Optional

import pandas as pd

from xerparser.schemas.actvtype import ACTVTYPE
from xerparser.schemas.calendars import CALENDAR
from xerparser.schemas.pcattype import PCATTYPE
from xerparser.schemas.pcatval import PCATVAL
from xerparser.schemas.projwbs import PROJWBS
from xerparser.schemas.schedoptions import SCHEDOPTIONS
from xerparser.schemas.task import TASK
from xerparser.schemas.taskpred import TASKPRED
from xerparser.schemas.taskrsrc import TASKRSRC
from xerparser.schemas.udftype import UDFTYPE
from xerparser.scripts.decorators import rounded
from xerparser.src.validators import date_format, optional_date, optional_str

class PROJECT:
    """
    A class representing a schedule.
    """

    def __init__(self, row: pd.Series, sched_options: SCHEDOPTIONS) -> None:
        self.uid: str = row['proj_id']
        """Unique Table ID"""
        self.fy_start_month_num: int = row['fy_start_month_num']
        """Fiscal Year Start Month"""
        self.chng_eff_cmp_pct_flag: bool = row['chng_eff_cmp_pct_flag'] == 'Y'
        """Change Effective Completion Percent Flag"""
        self.rsrc_self_add_flag: bool = row['rsrc_self_add_flag'] == 'Y'
        """Resource Self Add Flag"""
        self.rsrc_role_match_flag: bool = row['rsrc_role_match_flag'] == 'Y'
        """Resource Role Match Flag"""
        self.allow_complete_flag: bool = row['allow_complete_flag'] == 'Y'
        """Allow Complete Flag"""
        self.rsrc_multi_assign_flag: bool = row['rsrc_multi_assign_flag'] == 'Y'
        """Resource Multi Assign Flag"""
        self.checkout_flag: bool = row['checkout_flag'] == 'Y'
        """Checkout Flag"""
        self.project_flag: bool = row['project_flag'] == 'Y'
        """Project Flag"""
        self.step_complete_flag: bool = row['step_complete_flag'] == 'Y'
        """Step Complete Flag"""
        self.cost_qty_recalc_flag: bool = row['cost_qty_recalc_flag'] == 'Y'
        """Cost Quantity Recalculation Flag"""
        self.sum_only_flag: bool = row['sum_only_flag'] == 'Y'
        """Summary Only Flag"""
        self.batch_sum_flag: bool = row['batch_sum_flag'] == 'Y'
        """Batch Summary Flag"""
        self.name_sep_char: str = row['name_sep_char']
        """Name Separator Character"""
        self.def_complete_pct_type: str = row['def_complete_pct_type']
        """Default Percent Complete Type"""
        self.short_name: str = row['proj_short_name']
        """Project Code"""
        self.acct_id: Optional[str] = optional_str(row['acct_id'])
        """Account ID"""
        self.orig_proj_id: Optional[str] = optional_str(row['orig_proj_id'])
        """Original Project ID"""
        self.source_proj_id: Optional[str] = optional_str(row['source_proj_id'])
        """Source Project ID"""
        self.base_type_id: Optional[str] = optional_str(row['base_type_id'])
        """Base Type ID"""
        self.clndr_id: Optional[str] = optional_str(row['clndr_id'])
        """Calendar ID"""
        self.sum_base_proj_id: Optional[str] = optional_str(row['sum_base_proj_id'])
        """Summary Base Project ID"""
        self.task_code_base: Optional[int] = optional_str(row['task_code_base'])
        """Task Code Base"""
        self.task_code_step: Optional[int] = optional_str(row['task_code_step'])
        """Task Code Step"""
        self.priority_num: Optional[int] = optional_str(row['priority_num'])
        """Priority Number"""
        self.wbs_max_sum_level: Optional[int] = optional_str(row['wbs_max_sum_level'])
        """WBS Maximum Summary Level"""
        self.risk_level: Optional[int] = optional_str(row['risk_level'])
        """Risk Level"""
        self.strgy_priority_num: Optional[int] = optional_str(row['strgy_priority_num'])
        """Strategy Priority Number"""
        self.last_checksum: Optional[int] = optional_str(row['last_checksum'])
        """Last Checksum"""
        self.critical_drtn_hr_cnt: Optional[float] = optional_str(row['critical_drtn_hr_cnt'])
        """Critical Duration Hours Count"""
        self.def_cost_per_qty: Optional[float] = optional_str(row['def_cost_per_qty'])
        """Default Cost per Quantity"""
        self.last_recalc_date: datetime = datetime.strptime(row['last_recalc_date'], date_format)
        """Last Recalculation Date"""
        self.plan_start_date: datetime = datetime.strptime(row['plan_start_date'], date_format)
        """Planned Start Date"""
        self.plan_end_date: datetime = datetime.strptime(row['plan_end_date'], date_format)
        """Planned End Date"""
        self.scd_end_date: datetime = datetime.strptime(row['scd_end_date'], date_format)
        """Scheduled End Date"""
        self.add_date: datetime = datetime.strptime(row['add_date'], date_format)
        """Added Date"""
        self.sum_data_date: Optional[datetime] = optional_date(row['sum_data_date'])
        """Summary Data Date"""
        self.last_tasksum_date: Optional[datetime] = optional_date(row['last_tasksum_date'])
        """Last Task Summary Date"""
        self.fcst_start_date: Optional[datetime] = optional_date(row['fcst_start_date'])
        """Forecast Start Date"""
        self.def_duration_type: Optional[str] = optional_str(row['def_duration_type'])
        """Default Duration Type"""
        self.task_code_prefix: Optional[str] = optional_str(row['task_code_prefix'])
        """Task Code Prefix"""
        self.guid: Optional[str] = optional_str(row['guid'])
        """Global Unique ID"""
        self.def_qty_type: Optional[str] = optional_str(row['def_qty_type'])
        """Default Quantity Type"""
        self.add_by_name: Optional[str] = optional_str(row['add_by_name'])
        """Added By Name"""
        self.web_local_root_path: Optional[str] = optional_str(row['web_local_root_path'])
        """Web Local Root Path"""
        self.proj_url: Optional[str] = optional_str(row['proj_url'])
        """Project URL"""
        self.def_rate_type: Optional[str] = optional_str(row['def_rate_type'])
        """Default Rate Type"""
        self.act_this_per_link_flag: bool = row['act_this_per_link_flag'] == 'Y'
        """Actual This Period Link Flag"""
        self.def_task_type: str = row['def_task_type']
        """Default Task Type"""
        self.act_pct_link_flag: bool = row['act_pct_link_flag'] == 'Y'
        """Actual Percent Link Flag"""
        self.add_act_remain_flag: bool = row['add_act_remain_flag'] == 'Y'
        """Add Actual Remaining Flag"""
        self.critical_path_type: str = row['critical_path_type']
        """Critical Path Type"""
        self.task_code_prefix_flag: bool = row['task_code_prefix_flag'] == 'Y'
        """Task Code Prefix Flag"""
        self.def_rollup_dates_flag: bool = row['def_rollup_dates_flag'] == 'Y'
        """Default Rollup Dates Flag"""
        self.rem_target_link_flag: bool = row['rem_target_link_flag'] == 'Y'
        """Remaining Target Link Flag"""
        self.reset_planned_flag: bool = row['reset_planned_flag'] == 'Y'
        """Reset Planned Flag"""
        self.allow_neg_act_flag: bool = row['allow_neg_act_flag'] == 'Y'
        """Allow Negative Actuals Flag"""
        self.rsrc_id: Optional[str] = optional_str(row['rsrc_id'])
        """Resource ID"""
        self.msp_managed_flag: bool = row['msp_managed_flag'] == 'Y'
        """Microsoft Project Managed Flag"""
        self.msp_update_actuals_flag: bool = row['msp_update_actuals_flag'] == 'Y'
        """Microsoft Project Update Actuals Flag"""
        self.checkout_date: Optional[datetime] = optional_date(row['checkout_date'])
        """Checkout Date"""
        self.checkout_user_id: Optional[str] = optional_str(row['checkout_user_id'])
        """Checkout User ID"""
        self.sum_assign_level: Optional[str] = optional_str(row['sum_assign_level'])
        """Summary Assignment Level"""
        self.last_fin_dates_id: Optional[str] = optional_str(row['last_fin_dates_id'])
        """Last Financial Dates ID"""
        self.use_project_baseline_flag: bool = row['use_project_baseline_flag'] == 'Y'
        """Use Project Baseline Flag"""
        self.last_baseline_update_date: Optional[datetime] = optional_date(row['last_baseline_update_date'])
        """Last Baseline Update Date"""
        self.ts_rsrc_vw_compl_asgn_flag: bool = row['ts_rsrc_vw_compl_asgn_flag'] == 'Y'
        """Timesheet Resource View Completed Assignments Flag"""
        self.ts_rsrc_mark_act_finish_flag: bool = row['ts_rsrc_mark_act_finish_flag'] == 'Y'
        """Timesheet Resource Mark Actual Finish Flag"""
        self.ts_rsrc_vw_inact_actv_flag: bool = row['ts_rsrc_vw_inact_actv_flag'] == 'Y'
        """Timesheet Resource View Inactive Activities Flag"""
        self.cr_external_key: Optional[str] = optional_str(row['cr_external_key'])
        """External Key"""
        self.apply_actuals_date: Optional[datetime] = optional_date(row['apply_actuals_date'])
        """Apply Actuals Date"""
        self.description: Optional[str] = optional_str(row['description'])
        """Description"""
        self.intg_proj_type: Optional[str] = optional_str(row['intg_proj_type'])
        """Integration Project Type"""
        self.matrix_id: Optional[str] = optional_str(row['matrix_id'])
        """Matrix ID"""
        self.location_id: Optional[str] = optional_str(row['location_id'])
        """Location ID"""
        self.last_schedule_date: Optional[datetime] = optional_date(row['last_schedule_date'])
        """Last Schedule Date"""
        self.control_updates_flag: bool = row['control_updates_flag'] == 'Y'
        """Control Updates Flag"""
        self.hist_interval: str = row['hist_interval']
        """History Interval"""
        self.hist_level: str = row['hist_level']
        """History Level"""
        self.fintmpl_id: Optional[str] = optional_str(row['fintmpl_id'])
        """Financial Template ID"""

        self.options: SCHEDOPTIONS = sched_options
        self.activity_codes: list[ACTVTYPE] = []
        self.calendars: list[CALENDAR] = []
        self.project_codes: dict[PCATTYPE, PCATVAL] = {}
        self.tasks: list[TASK] = []
        self.relationships: list[TASKPRED] = []
        self.resources: list[TASKRSRC] = []
        self.wbs_nodes: list[PROJWBS] = []
        self.user_defined_fields: dict[UDFTYPE, Any] = {}

    def __str__(self) -> str:
        return f"{self.short_name} - {self.name}"

    @cached_property
    @rounded()
    def actual_cost(self) -> float:
        """Sum of task resource actual costs"""
        return sum(res.act_total_cost for res in self.resources)

    @property
    def actual_duration(self) -> int:
        """Project actual duration in calendar days from start date to data date"""
        return max((0, (self.data_date - self.actual_start).days))

    @cached_property
    def actual_start(self) -> datetime:
        """Earliest task start date"""
        if not self.tasks:
            return self.plan_start_date
        return min((task.start for task in self.tasks))

    @cached_property
    @rounded()
    def budgeted_cost(self) -> float:
        """Sum of task resource budgeted costs"""
        return sum(res.target_cost for res in self.resources)

    @property
    @rounded(ndigits=4)
    def duration_percent(self) -> float:
        """Project duration percent complete"""
        if self.original_duration == 0:
            return 0.0

        if self.data_date >= self.finish_date:
            return 1.0

        return 1 - self.remaining_duration / self.original_duration

    @cached_property
    def finish_constraints(self) -> list[tuple[TASK, str]]:
        """List of all Tasks with Finish on or Before constraints"""
        return sorted(
            [
                (task, cnst)
                for task in self.tasks
                for cnst in ("prime", "second")
                if task.constraints[cnst]["type"] is TASK.ConstraintType.CS_MEOB
            ],
            key=lambda t: t[0].finish,
        )

    @cached_property
    def late_start(self) -> datetime:
        """Earliest task late start date"""
        if not self.tasks:
            return self.plan_start_date
        return min(
            (task.late_start_date for task in self.tasks if task.late_start_date)
        )

    @property
    def name(self) -> str:
        """Project Name"""
        if not self.wbs_nodes:
            return ""
        return self.wbs_nodes[0].name

    @property
    def original_duration(self) -> int:
        """
        Project overall duration in calendar days
        from actual start date to finish date
        """
        return (self.finish_date - self.actual_start).days

    @cached_property
    def relationships_by_hash(self) -> dict[int, TASKPRED]:
        return {hash(rel): rel for rel in self.relationships}

    @cached_property
    @rounded()
    def remaining_cost(self) -> float:
        """Sum of task resource remaining costs"""
        return sum(res.remain_cost for res in self.resources)

    @property
    def remaining_duration(self) -> int:
        """Project remaining duration in calendar days from data date to finish date"""
        return max((0, (self.finish_date - self.data_date).days))

    @cached_property
    @rounded(ndigits=4)
    def task_percent(self) -> float:
        """
        Project percent complete based on task updates.
        Calculated using the median of the following 2 ratios:

        * Ratio between Actual Dates and Activity Count.
        `(Actual Start Count + Actual Finish Count) ÷ (Activity Count * 2)`
        * Ratio between Sum of Task Remaining Durations and Task Original Durations.
        `1 - (sum of task remaining duration ÷ sum of task original duration)`
        """
        if not self.tasks:
            return 0.0

        orig_dur_sum = sum(
            task.original_duration
            for task in self.tasks
            if not any([task.type.is_loe, task.type.is_wbs])
        )
        rem_dur_sum = sum(
            task.remaining_duration
            for task in self.tasks
            if not any([task.type.is_loe, task.type.is_wbs])
        )
        task_dur_percent = 1 - rem_dur_sum / orig_dur_sum if orig_dur_sum else 0.0

        status_cnt = Counter([t.status for t in self.tasks])
        status_percent = (
                                 status_cnt[TASK.TaskStatus.TK_Active] / 2
                                 + status_cnt[TASK.TaskStatus.TK_Complete]
                         ) / len(self.tasks)

        return mean([task_dur_percent, status_percent])

    @cached_property
    def tasks_by_code(self) -> dict[str, TASK]:
        """
        Returns a dictionary of the Activities using the
        Activity ID as the key and the TASK object as the value.
        """
        return {task.task_code: task for task in self.tasks}

    @cached_property
    @rounded()
    def this_period_cost(self) -> float:
        """Sum of task resource this period costs"""
        return sum(res.act_this_per_cost for res in self.resources)

    @cached_property
    def wbs_by_path(self) -> dict[str, PROJWBS]:
        return {node.full_code: node for node in self.wbs_nodes}

    @property
    def wbs_root(self) -> PROJWBS:
        if not self.wbs_nodes:
            raise UnboundLocalError("WBS Root is not assigned")

        return self.wbs_nodes[0]

    def planned_progress(self, before_date: datetime) -> dict[str, list[TASK]]:
        """All planned progress through a given date.

        Args:
            before_date (datetime): End date for planned progress

        Returns:
            dict[str, list[TASK]]: Early and late planned progress during time frame
        """
        progress = {"start": [], "finish": [], "late_start": [], "late_finish": []}

        if before_date < self.data_date:
            return progress

        for task in self.tasks:
            if task.status.is_completed:
                continue

            if task.status.is_not_started:
                if task.start < before_date:
                    progress["start"].append(task)

                if task.late_start_date and task.late_start_date < before_date:
                    progress["late_start"].append(task)

            if task.finish < before_date:
                progress["finish"].append(task)

            if task.late_end_date and task.late_end_date < before_date:
                progress["late_finish"].append(task)

        return progress

    @property
    def data_date(self) -> datetime:
        """
        The date the project data is current to.
        This is the latest of the last schedule date, or the last task summary date.
        """
        return max(
            [d for d in [self.last_schedule_date, self.last_tasksum_date] if d]
        )