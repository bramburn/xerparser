# xerparser
# rsrc.py

import pandas as pd
from typing import Any
from xerparser.schemas.udftype import UDFTYPE

class RSRC:
    """
    A class to represent a Resource.
    """

    def __init__(self, rsrc_df: pd.DataFrame) -> None:
        self.rsrc_df = rsrc_df
        self.rsrc_dict = self._process_rsrc_data(rsrc_df)

    @staticmethod
    def _process_rsrc_data(rsrc_df: pd.DataFrame) -> dict[str, dict]:
        rsrc_dict = {}
        for _, row in rsrc_df.iterrows():
            rsrc = {
                "uid": row["rsrc_id"],
                "name": row["rsrc_name"],
                "short_name": row["rsrc_short_name"],
                "parent_id": row["parent_rsrc_id"],
                "clndr_id": row["clndr_id"],
                "type": row["rsrc_type"],
                "user_defined_fields": {}
            }
            rsrc_dict[rsrc["uid"]] = rsrc
        return rsrc_dict

    def get_rsrc(self, rsrc_id: str) -> dict:
        return self.rsrc_dict[rsrc_id]

    @property
    def lineage(self) -> list["RSRC"]:
        if not self.rsrc_dict[self.uid]["parent_id"]:
            return [self]

        parent = self.get_rsrc(self.rsrc_dict[self.uid]["parent_id"])
        return parent.lineage + [self]

    @property
    def full_code(self) -> str:
        return ".".join([node.rsrc_dict[node.uid]["short_name"] for node in self.lineage])

    def __hash__(self) -> int:
        return hash(self.uid)

    def __eq__(self, other: "RSRC") -> bool:
        return self.uid == other.uid

    def __repr__(self) -> str:
        return f"RSRC(uid={self.uid}, name={self.rsrc_dict[self.uid]['name']})"