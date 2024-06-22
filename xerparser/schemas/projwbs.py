# xerparser
# projwbs.py

import pandas as pd
from typing import Any, Dict

from xerparser.schemas.udftype import UDFTYPE
from xerparser.src.validators import optional_int


class PROJWBS:
    """
    A class to represent a schedule WBS node.
    """

    def __init__(self, wbs_data: pd.Series) -> None:
        self.uid: str = wbs_data['wbs_id']
        self.code: str = wbs_data['wbs_short_name']
        self.name: str = wbs_data['wbs_name']
        self.parent_id: str = wbs_data['parent_wbs_id']
        self.is_proj_node: bool = wbs_data['proj_node_flag'] == 'Y'
        """Project Level Code Flag"""
        self.proj_id: str = wbs_data['proj_id']
        """Foreign Key for `PROJECT` WBS node belongs to"""
        self.seq_num: int | None = optional_int(wbs_data['seq_num'])
        """Sort Order"""
        self.status_code: str = wbs_data['status_code']

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
