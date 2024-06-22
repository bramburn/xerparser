# xerparser
# projwbs.py

import pandas as pd
from typing import Any, Dict, Optional

from xerparser.schemas.udftype import UDFTYPE
from xerparser.src.validators import optional_int, optional_str, optional_date


class PROJWBS:
    """
    A class to represent a schedule WBS node.
    """

    def __init__(self, row: pd.Series) -> None:
        self.uid: str = row['wbs_id']
        self.code: str = row['wbs_short_name']
        self.name: str = row['wbs_name']
        self.parent_id: Optional[str] = optional_str(row['parent_wbs_id'])
        self.is_proj_node: bool = row['proj_node_flag'] == 'Y'
        """Project Level Code Flag"""
        self.proj_id: str = row['proj_id']
        """Foreign Key for `PROJECT` WBS node belongs to"""
        self.seq_num: Optional[int] = optional_int(row['seq_num'])
        """Sort Order"""
        self.status_code: str = row['status_code']
        self.est_wt: Optional[float] = optional_int(row['est_wt'])
        """Estimated Weight"""
        self.sum_data_flag: bool = row['sum_data_flag'] == 'Y'
        """Summary Data Flag"""
        self.ev_user_pct: Optional[int] = optional_int(row['ev_user_pct'])
        """Earned Value User Percent"""
        self.ev_etc_user_value: Optional[float] = optional_int(row['ev_etc_user_value'])
        """Earned Value ETC User Value"""
        self.orig_cost: Optional[float] = optional_int(row['orig_cost'])
        """Original Cost"""
        self.indep_remain_total_cost: Optional[float] = optional_int(row['indep_remain_total_cost'])
        """Independent Remaining Total Cost"""
        self.ann_dscnt_rate_pct: Optional[float] = optional_int(row['ann_dscnt_rate_pct'])
        """Annual Discount Rate Percentage"""
        self.dscnt_period_type: Optional[str] = optional_str(row['dscnt_period_type'])
        """Discount Period Type"""
        self.indep_remain_work_qty: Optional[float] = optional_int(row['indep_remain_work_qty'])
        """Independent Remaining Work Quantity"""
        self.anticip_start_date: Optional[pd.Timestamp] = optional_date(row['anticip_start_date'])
        """Anticipated Start Date"""
        self.anticip_end_date: Optional[pd.Timestamp] = optional_date(row['anticip_end_date'])
        """Anticipated End Date"""
        self.ev_compute_type: Optional[str] = optional_str(row['ev_compute_type'])
        """Earned Value Computation Type"""
        self.ev_etc_compute_type: Optional[str] = optional_str(row['ev_etc_compute_type'])
        """Earned Value ETC Computation Type"""
        self.resp_team_id: Optional[int] = optional_int(row['resp_team_id'])
        """Responsible Team ID"""
        self.iteration_id: Optional[int] = optional_int(row['iteration_id'])
        """Iteration ID"""
        self.guid: Optional[str] = optional_str(row['guid'])
        """Global Unique ID"""
        self.tmpl_guid: Optional[str] = optional_str(row['tmpl_guid'])
        """Template Global Unique ID"""
        self.original_qty: Optional[float] = optional_int(row['original_qty'])
        """Original Quantity"""
        self.rqmt_rem_qty: Optional[float] = optional_int(row['rqmt_rem_qty'])
        """Requirement Remaining Quantity"""
        self.intg_type: Optional[str] = optional_str(row['intg_type'])
        """Integration Type"""
        self.status_reviewer: Optional[int] = optional_int(row['status_reviewer'])
        """Status Reviewer"""

        self.assignments: int = 0
        """Activity Assignment Count"""
        self.user_defined_fields: Dict[UDFTYPE, Any] = {}

    @property
    def lineage(self) -> list["PROJWBS"]:
        if self.is_proj_node:
            return []

        if not self.parent_id:
            return [self]

        return self.parent_lineage + [self]

    @property
    def parent_lineage(self) -> list["PROJWBS"]:
        if not self.parent_id:
            return []

        parent = self.project.wbs_nodes[self.parent_id]
        return parent.lineage

    @property
    def full_code(self) -> str:
        return ".".join([node.code for node in self.lineage])

    def __hash__(self) -> int:
        return hash(self.uid)

    def __eq__(self, other: "PROJWBS") -> bool:
        return self.uid == other.uid

    def __repr__(self) -> str:
        return f"PROJWBS(uid={self.uid}, name={self.name})"


def _process_projwbs_data(projwbs_df: pd.DataFrame) -> Dict[str, PROJWBS]:
    wbs_nodes = {}
    for _, row in projwbs_df.iterrows():
        wbs_node = PROJWBS(row)
        wbs_nodes[wbs_node.uid] = wbs_node
    return wbs_nodes