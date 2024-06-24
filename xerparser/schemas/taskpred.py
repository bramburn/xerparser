# taskpred.py
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from xerparser.src.validators import optional_int, optional_str

@dataclass(frozen=True)
class TASKPRED:
    """
    A class to represent an Activity Relationship.
    """
    task_pred_id: str
    """Unique ID"""
    task_id: str
    """Successor"""
    pred_task_id: str
    """Predecessor"""
    proj_id: str
    """Successor Project"""
    pred_proj_id: str
    """Predecessor Project"""
    lag_hr_cnt: int
    """Lag"""
    comments: Optional[str]
    """Comments"""
    pred_type: str
    """Relationship Type"""

    def __eq__(self, __o: "TASKPRED") -> bool:
        return self.task_pred_id == __o.task_pred_id

    def __hash__(self) -> int:
        return hash(self.task_pred_id)

def _process_taskpred_data(taskpred_df: pd.DataFrame) -> dict[str, TASKPRED]:
    taskpred_dict = {}
    for _, row in taskpred_df.iterrows():
        taskpred = TASKPRED(
            task_pred_id=row["task_pred_id"],
            task_id=row["task_id"],
            pred_task_id=row["pred_task_id"],
            proj_id=row["proj_id"],
            pred_proj_id=row["pred_proj_id"],
            lag_hr_cnt=optional_int(row["lag_hr_cnt"]),
            comments=optional_str(row["comments"]),
            pred_type=row["pred_type"]
        )
        taskpred_dict[taskpred.task_pred_id] = taskpred
    return taskpred_dict