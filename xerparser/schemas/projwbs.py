# xerparser
# projwbs.py

import pandas as pd
from typing import Any, Dict, Optional

from xerparser.schemas.udftype import UDFTYPE
from xerparser.src.validators import optional_int, optional_str, optional_date, optional_float


class PROJWBS:
    """
    A class to represent a schedule WBS node.
    """

    def __init__(self, projwbs_df: pd.DataFrame) -> None:
        self.projwbs_df = projwbs_df
        self.projwbs_dict = self._process_projwbs_data(projwbs_df)

    def _process_projwbs_data(self, projwbs_df: pd.DataFrame) -> dict[str, "PROJWBS"]:
        projwbs_dict = {}
        for _, row in projwbs_df.iterrows():
            projwbs = {
                "uid": row['wbs_id'],
                "code": row['wbs_short_name'],
                "name": row['wbs_name'],
                "parent_id": optional_str(row['parent_wbs_id']),
                "is_proj_node": row['proj_node_flag'] == 'Y',
                "proj_id": row['proj_id'],
                "seq_num": optional_int(row['seq_num']),
                "status_code": row['status_code'],
                "est_wt": optional_float(row['est_wt']),
                "sum_data_flag": row['sum_data_flag'] == 'Y',
                "ev_user_pct": optional_int(row['ev_user_pct']),
                "ev_etc_user_value": optional_float(row['ev_etc_user_value']),
                "orig_cost": optional_float(row['orig_cost']),
                "indep_remain_total_cost": optional_float(row['indep_remain_total_cost']),
                "ann_dscnt_rate_pct": optional_float(row['ann_dscnt_rate_pct']),
                "dscnt_period_type": optional_str(row['dscnt_period_type']),
                "indep_remain_work_qty": optional_float(row['indep_remain_work_qty']),
                "anticip_start_date": optional_date(row['anticip_start_date']),
                "anticip_end_date": optional_date(row['anticip_end_date']),
                "ev_compute_type": optional_str(row['ev_compute_type']),
                "ev_etc_compute_type": optional_str(row['ev_etc_compute_type']),
                "resp_team_id": optional_int(row['resp_team_id']),
                "iteration_id": optional_int(row['iteration_id']),
                "guid": optional_str(row['guid']),
                "tmpl_guid": optional_str(row['tmpl_guid']),
                "original_qty": optional_float(row['original_qty']),
                "rqmt_rem_qty": optional_float(row['rqmt_rem_qty']),
                "intg_type": optional_str(row['intg_type']),
                "status_reviewer": optional_int(row['status_reviewer'])
            }
            projwbs_dict[projwbs["uid"]] = PROJWBS(projwbs)
        return projwbs_dict

    def get_projwbs(self, wbs_id: str) -> "PROJWBS":
        return self.projwbs_dict[wbs_id]

    @property
    def lineage(self) -> list["PROJWBS"]:
        if self.projwbs_dict[self.uid]["is_proj_node"]:
            return []

        if self.projwbs_dict[self.uid]["parent_id"] is None:
            return [self]

        return self.parent_lineage + [self]

    @property
    def parent_lineage(self) -> list["PROJWBS"]:
        if self.projwbs_dict[self.uid]["parent_id"] is None:
            return []

        parent = self.projwbs_dict[self.projwbs_dict[self.uid]["parent_id"]]
        return parent.lineage

    @property
    def full_code(self) -> str:
        return ".".join([self.projwbs_dict[node.uid]["code"] for node in self.lineage])

    def __hash__(self) -> int:
        return hash(self.uid)

    def __eq__(self, other: "PROJWBS") -> bool:
        return self.uid == other.uid

    def __repr__(self) -> str:
        return f"PROJWBS(uid={self.uid}, name={self.projwbs_dict[self.uid]['name']})"


def _process_projwbs_data(projwbs_df: pd.DataFrame) -> Dict[str, PROJWBS]:
    return PROJWBS(projwbs_df).projwbs_dict
