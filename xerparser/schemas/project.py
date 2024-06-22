# xerparser
# taskpred.py

from datetime import datetime
import pandas as pd

from xerparser.src.validators import optional_int, optional_date, int_or_zero, optional_str

class TASKPRED:
    """
    A class to represent a relationship between two activities.
    """

    def __init__(self, row: pd.Series) -> None:
        self.uid: str = row['task_pred_id']
        self.task_id: str = row['task_id']
        self.pred_task_id: str = row['pred_task_id']
        self.proj_id: str = row['proj_id']
        self.pred_proj_id: str = row['pred_proj_id']
        self.pred_type: str = row['pred_type']
        self.lag_hr_cnt: int = int_or_zero(row['lag_hr_cnt'])
        self.float_path: int | None = optional_int(row['float_path'])
        self.aref: datetime | None = optional_date(row['aref'])
        self.arls: datetime | None = optional_date(row['arls'])
        self.comments: str | None = optional_str(row['comments'])

    def __eq__(self, __o: "TASKPRED") -> bool:
        return (
            self.task_id == __o.task_id
            and self.pred_task_id == __o.pred_task_id
            and self.link == __o.link
        )

    def __gt__(self, __o: "TASKPRED") -> bool:
        if self.task_id == __o.task_id:
            if self.pred_task_id == __o.pred_task_id:
                return self.link > __o.link
            return self.pred_task_id > __o.pred_task_id
        return self.task_id > __o.task_id

    def __lt__(self, __o: "TASKPRED") -> bool:
        if self.task_id == __o.task_id:
            if self.pred_task_id == __o.pred_task_id:
                return self.link < __o.link
            return self.pred_task_id < __o.pred_task_id
        return self.task_id < __o.task_id

    def __hash__(self) -> int:
        return hash((self.task_id, self.pred_task_id, self.link))

    @property
    def lag(self) -> int:
        return int(self.lag_hr_cnt / 8)

    @property
    def link(self) -> str:
        return self.pred_type[-2:]