# xerparser
# pcattype.py

import pandas as pd

class PCATTYPE:
    """
    A class representing Project Code Types
    """

    def __init__(self, pcattype_df: pd.DataFrame) -> None:
        self.pcattype_df = pcattype_df
        self.pcattype_dict = self._process_pcattype_data(pcattype_df)

    @staticmethod
    def _process_pcattype_data(pcattype_df: pd.DataFrame) -> dict[str, dict]:
        pcattype_dict = {}
        for _, row in pcattype_df.iterrows():
            pcattype = {
                "uid": row["proj_catg_type_id"],
                "max_length": int(row["proj_catg_short_len"]),
                "name": row["proj_catg_type"],
                "seq_num": int(row["seq_num"]) if not pd.isnull(row["seq_num"]) else None
            }
            pcattype_dict[pcattype["uid"]] = pcattype
        return pcattype_dict

    def get_pcattype(self, pcattype_id: str) -> dict:
        return self.pcattype_dict[pcattype_id]

    def __eq__(self, __o: "PCATTYPE") -> bool:
        if __o is None:
            return False
        return all(
            (
                self.pcattype_dict[__o.uid]["max_length"] == self.pcattype_dict[self.uid]["max_length"],
                self.pcattype_dict[__o.uid]["name"] == self.pcattype_dict[self.uid]["name"],
            )
        )

    def __gt__(self, __o: "PCATTYPE") -> bool:
        return self.pcattype_dict[__o.uid]["name"] > self.pcattype_dict[self.uid]["name"]

    def __lt__(self, __o: "PCATTYPE") -> bool:
        return self.pcattype_dict[__o.uid]["name"] < self.pcattype_dict[self.uid]["name"]

    def __hash__(self) -> int:
        return hash((self.pcattype_dict[self.uid]["max_length"], self.pcattype_dict[self.uid]["name"]))