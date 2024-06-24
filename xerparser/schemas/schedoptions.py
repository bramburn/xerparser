# xerparser
# schedoptions.py

import pandas as pd
from xerparser.src.validators import optional_int

class SCHEDOPTIONS:
    def __init__(self, schedoptions_df: pd.DataFrame) -> None:
        """
        A class to represent the Schedule Options.
        """
        self.schedoptions_df = schedoptions_df
        self.schedoptions_dict = self._process_schedoptions_data(schedoptions_df)

    def _process_schedoptions_data(self, schedoptions_df: pd.DataFrame) -> dict[str, dict]:
        schedoptions_dict = {}
        for _, row in schedoptions_df.iterrows():
            schedoptions = {
                "max_multiple_longest_path": optional_int(row["max_multiple_longest_path"]),
                "proj_id": row["proj_id"],
                "calendar_on_relationship_lag": row["sched_calendar_on_relationship_lag"],
                "float_type": row["sched_float_type"],
                "lag_early_start_flag": row["sched_lag_early_start_flag"] == "Y",
                "open_critical_flag": row["sched_open_critical_flag"] == "Y",
                "outer_depend_type": row["sched_outer_depend_type"],
                "progress_override": row["sched_progress_override"] == "Y",
                "retained_logic": row["sched_retained_logic"] == "Y",
                "setplantoforecast": row["sched_setplantoforecast"] == "Y",
                "use_expect_end_flag": row["sched_use_expect_end_flag"] == "Y",
                "use_project_end_date_for_float": row["sched_use_project_end_date_for_float"] == "Y",
                "schedoptions_id": row["schedoptions_id"],
                "use_total_float_multiple_longest_paths": row["use_total_float_multiple_longest_paths"] == "Y"
            }
            schedoptions_dict[schedoptions["schedoptions_id"]] = schedoptions
        return schedoptions_dict

    def get_schedoptions(self, schedoptions_id: str) -> dict:
        return self.schedoptions_dict[schedoptions_id]

    def __eq__(self, __o: "SCHEDOPTIONS") -> bool:
        if __o is None:
            return False
        return all(
            (
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["max_multiple_longest_path"] == self.schedoptions_dict[__o.schedoptions_df.iloc[0]["schedoptions_id"]]["max_multiple_longest_path"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["calendar_on_relationship_lag"] == self.schedoptions_dict[__o.schedoptions_df.iloc[0]["schedoptions_id"]]["calendar_on_relationship_lag"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["float_type"] == self.schedoptions_dict[__o.schedoptions_df.iloc[0]["schedoptions_id"]]["float_type"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["lag_early_start_flag"] == self.schedoptions_dict[__o.schedoptions_df.iloc[0]["schedoptions_id"]]["lag_early_start_flag"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["open_critical_flag"] == self.schedoptions_dict[__o.schedoptions_df.iloc[0]["schedoptions_id"]]["open_critical_flag"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["outer_depend_type"] == self.schedoptions_dict[__o.schedoptions_df.iloc[0]["schedoptions_id"]]["outer_depend_type"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["progress_override"] == self.schedoptions_dict[__o.schedoptions_df.iloc[0]["schedoptions_id"]]["progress_override"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["retained_logic"] == self.schedoptions_dict[__o.schedoptions_df.iloc[0]["schedoptions_id"]]["retained_logic"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["setplantoforecast"] == self.schedoptions_dict[__o.schedoptions_df.iloc[0]["schedoptions_id"]]["setplantoforecast"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["use_expect_end_flag"] == self.schedoptions_dict[__o.schedoptions_df.iloc[0]["schedoptions_id"]]["use_expect_end_flag"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["use_project_end_date_for_float"] == self.schedoptions_dict[__o.schedoptions_df.iloc[0]["schedoptions_id"]]["use_project_end_date_for_float"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["use_total_float_multiple_longest_paths"] == self.schedoptions_dict[__o.schedoptions_df.iloc[0]["schedoptions_id"]]["use_total_float_multiple_longest_paths"]
            )
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["max_multiple_longest_path"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["calendar_on_relationship_lag"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["float_type"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["lag_early_start_flag"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["open_critical_flag"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["outer_depend_type"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["progress_override"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["retained_logic"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["setplantoforecast"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["use_expect_end_flag"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["use_project_end_date_for_float"],
                self.schedoptions_dict[self.schedoptions_df.iloc[0]["schedoptions_id"]]["use_total_float_multiple_longest_paths"]
            )
        )