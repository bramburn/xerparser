# xerparser
# findates.py

import pandas as pd
from xerparser.src.validators import date_format

class FINDATES:
    """
    A class representing a Financial Period
    """

    def __init__(self, findates_df: pd.DataFrame) -> None:
        self.findates_df = findates_df
        self.findates_dict = self._process_findates_data(findates_df)

    def _process_findates_data(self, findates_df: pd.DataFrame) -> dict[str, dict]:
        findates_dict = {}
        for _, row in findates_df.iterrows():
            findates = {
                "uid": row["fin_dates_id"],
                "name": row["fin_dates_name"],
                "start_date": pd.to_datetime(row["start_date"], format=date_format),
                "end_date": pd.to_datetime(row["end_date"], format=date_format)
            }
            findates_dict[findates["uid"]] = findates
        return findates_dict

    def get_findates(self, findates_id: str) -> dict:
        return self.findates_dict[findates_id]

    def __eq__(self, __o: "FINDATES") -> bool:
        if __o is None:
            return False
        return self.findates_dict[__o.uid]["start_date"] == self.findates_dict[__o.uid]["start_date"] and \
               self.findates_dict[__o.uid]["end_date"] == self.findates_dict[__o.uid]["end_date"]

    def __gt__(self, __o: "FINDATES") -> bool:
        if self.findates_dict[__o.uid]["start_date"] == self.findates_dict[__o.uid]["start_date"]:
            return self.findates_dict[__o.uid]["end_date"] > self.findates_dict[__o.uid]["end_date"]
        return self.findates_dict[__o.uid]["start_date"] > self.findates_dict[__o.uid]["start_date"]

    def __lt__(self, __o: "FINDATES") -> bool:
        if self.findates_dict[__o.uid]["start_date"] == self.findates_dict[__o.uid]["start_date"]:
            return self.findates_dict[__o.uid]["end_date"] < self.findates_dict[__o.uid]["end_date"]
        return self.findates_dict[__o.uid]["start_date"] < self.findates_dict[__o.uid]["start_date"]

    def __hash__(self) -> int:
        return hash((self.findates_dict[self.uid]["name"], self.findates_dict[self.uid]["start_date"], self.findates_dict[self.uid]["end_date"]))