# phase.py
from dataclasses import dataclass
from typing import Optional

import pandas as pd

@dataclass(frozen=True)
class PHASE:
    """
    A class to represent a Project Code Type (PHASE).
    """
    phase_id: str
    """Unique ID"""
    phase_name: str
    """Category Value"""
    seq_num: Optional[int] = None
    """Sort Order"""

    def __eq__(self, __o: "PHASE") -> bool:
        return self.phase_name == __o.phase_name

    def __gt__(self, __o: "PHASE") -> bool:
        return self.phase_name > __o.phase_name

    def __lt__(self, __o: "PHASE") -> bool:
        return self.phase_name < __o.phase_name

    def __hash__(self) -> int:
        return hash(self.phase_name)

def _process_phase_data(phase_df: pd.DataFrame) -> dict[str, PHASE]:
    phase_dict = {}
    for _, row in phase_df.iterrows():
        phase = PHASE(
            phase_id=row["phase_id"],
            phase_name=row["phase_name"],
            seq_num=int(row["seq_num"]) if not pd.isnull(row["seq_num"]) else None
        )
        phase_dict[phase.phase_id] = phase
    return phase_dict