# xerparser
# pcatval.py

import pandas as pd
from xerparser.schemas._node import Node
from xerparser.schemas.pcattype import PCATTYPE

class PCATVAL(Node):
    """
    A class to represent an Project Code Value
    """

    def __init__(self, pcatval_df: pd.DataFrame, pcattype: PCATTYPE) -> None:
        self.pcatval_df = pcatval_df
        self.pcatval_dict = self._process_pcatval_data(pcatval_df, pcattype)

    @staticmethod
    def _process_pcatval_data(pcatval_df: pd.DataFrame, pcattype: PCATTYPE) -> dict[str, dict]:
        pcatval_dict = {}
        for _, row in pcatval_df.iterrows():
            pcatval = {
                "uid": row["proj_catg_id"],
                "name": row["proj_catg_name"],
                "short_name": row["proj_catg_short_name"],
                "parent_id": row["parent_proj_catg_id"],
                "proj_catg_type_id": row["proj_catg_type_id"],
                "seq_num": int(row["seq_num"]),
                "code_type": pcattype
            }
            pcatval_dict[pcatval["uid"]] = pcatval
        return pcatval_dict

    def get_pcatval(self, pcatval_id: str) -> dict:
        return self.pcatval_dict[pcatval_id]

    def __eq__(self, __o: "PCATVAL") -> bool:
        if __o is None:
            return False
        return self.pcatval_dict[__o.uid]["full_code"] == self.pcatval_dict[self.uid]["full_code"] and \
               self.pcatval_dict[__o.uid]["code_type"] == self.pcatval_dict[self.uid]["code_type"]

    def __hash__(self) -> int:
        return hash((self.pcatval_dict[self.uid]["full_code"], self.pcatval_dict[self.uid]["code_type"]))

    def _valid_pcattype(self, value: PCATTYPE) -> PCATTYPE:
        """Validate Activity Code Type"""
        if not isinstance(value, PCATTYPE):
            raise ValueError(
                f"ValueError: expected <class PCATTYPE>; got {type(value)}"
            )
        if value.uid != self.pcatval_dict[self.uid]["proj_catg_type_id"]:
            raise ValueError(
                f"ValueError: Unique ID {value.uid} does not match proj_catg_type_id {self.pcatval_dict[self.uid]['proj_catg_type_id']}"  # noqa: E501
            )
        return value