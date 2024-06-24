# xerparser
# rsrcrate.py

import pandas as pd
from datetime import datetime
from xerparser.src.validators import date_format, float_or_zero

class Rate:
    """Units per Time Limits"""

    def __init__(self, rsrcrate_df: pd.DataFrame) -> None:
        self.rsrcrate_df = rsrcrate_df
        self.rsrcrate_dict = self._process_rsrcrate_data(rsrcrate_df)

    @staticmethod
    def _process_rsrcrate_data(rsrcrate_df: pd.DataFrame) -> dict[str, dict]:
        rsrcrate_dict = {}
        for _, row in rsrcrate_df.iterrows():
            rsrcrate = {
                "cost_per_qty": float_or_zero(row["cost_per_qty"]),
                "cost_per_qty2": float_or_zero(row["cost_per_qty2"]),
                "cost_per_qty3": float_or_zero(row["cost_per_qty3"]),
                "cost_per_qty4": float_or_zero(row["cost_per_qty4"]),
                "cost_per_qty5": float_or_zero(row["cost_per_qty5"]),
                "max_qty_per_hr": float(row["max_qty_per_hr"]),
                "start_date": pd.to_datetime(row["start_date"], format=date_format)
            }
            rsrcrate_dict[str(rsrcrate["start_date"])] = rsrcrate
        return rsrcrate_dict

    def get_rate(self, start_date: datetime) -> dict:
        return self.rsrcrate_dict[str(start_date)]

    def __eq__(self, __other: "Rate") -> bool:
        if __other is None:
            return False
        return self.rsrcrate_dict[str(self.rsrcrate_df.iloc[0]["start_date"])]["start_date"] == \
               self.rsrcrate_dict[str(__other.rsrcrate_df.iloc[0]["start_date"])]["start_date"]

    def __gt__(self, __other: "Rate") -> bool:
        return self.rsrcrate_dict[str(self.rsrcrate_df.iloc[0]["start_date"])]["start_date"] > \
               self.rsrcrate_dict[str(__other.rsrcrate_df.iloc[0]["start_date"])]["start_date"]

    def __hash__(self) -> int:
        return hash(self.rsrcrate_dict[str(self.rsrcrate_df.iloc[0]["start_date"])]["start_date"])

    def __lt__(self, __other: "Rate") -> bool:
        return self.rsrcrate_dict[str(self.rsrcrate_df.iloc[0]["start_date"])]["start_date"] < \
               self.rsrcrate_dict[str(__other.rsrcrate_df.iloc[0]["start_date"])]["start_date"]