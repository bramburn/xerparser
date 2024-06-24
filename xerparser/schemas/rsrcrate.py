# xerparser
# rsrcrate.py

import pandas as pd
from xerparser.schemas.rate import Rate
from xerparser.schemas.rsrc import RSRC


class RSRCRATE(Rate):
    def __init__(self, rsrcrate_df: pd.DataFrame, rsrc: RSRC) -> None:
        self.rsrcrate_df = rsrcrate_df
        self.rsrcrate_dict = self._process_rsrcrate_data(rsrcrate_df, rsrc)

    def _process_rsrcrate_data(self, rsrcrate_df: pd.DataFrame, rsrc: RSRC) -> dict[str, dict]:
        rsrcrate_dict = {}
        from xerparser.src.validators import float_or_zero
        from xerparser.src.validators import date_format
        for _, row in rsrcrate_df.iterrows():
            rsrcrate = {
                "uid": row["rsrc_rate_id"],
                "rsrc_id": row["rsrc_id"],
                "resource": rsrc,
                "shift_period_id": row["shift_period_id"],
                "cost_per_qty": float_or_zero(row["cost_per_qty"]),
                "cost_per_qty2": float_or_zero(row["cost_per_qty2"]),
                "cost_per_qty3": float_or_zero(row["cost_per_qty3"]),
                "cost_per_qty4": float_or_zero(row["cost_per_qty4"]),
                "cost_per_qty5": float_or_zero(row["cost_per_qty5"]),
                "max_qty_per_hr": float(row["max_qty_per_hr"]),
                "start_date": pd.to_datetime(row["start_date"], format=date_format)
            }
            rsrcrate_dict[rsrcrate["uid"]] = rsrcrate
        return rsrcrate_dict

    def get_rsrcrate(self, rsrcrate_id: str) -> dict:
        return self.rsrcrate_dict[rsrcrate_id]

    def __eq__(self, __other: "RSRCRATE") -> bool:
        if __other is None:
            return False
        return self.rsrcrate_dict[self.uid]["start_date"] == self.rsrcrate_dict[__other.uid]["start_date"]

    def __gt__(self, __other: "RSRCRATE") -> bool:
        return self.rsrcrate_dict[self.uid]["start_date"] > self.rsrcrate_dict[__other.uid]["start_date"]

    def __hash__(self) -> int:
        return hash(self.rsrcrate_dict[self.uid]["start_date"])

    def __lt__(self, __other: "RSRCRATE") -> bool:
        return self.rsrcrate_dict[self.uid]["start_date"] < self.rsrcrate_dict[__other.uid]["start_date"]
