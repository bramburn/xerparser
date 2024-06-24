# projpcat.py
from dataclasses import dataclass
from typing import Optional

import pandas as pd

@dataclass(frozen=True)
class PROJPCAT:
    """
    A class to represent a Project Code Assignment.
    """
    proj_catg_id: str
    """Code Value"""
    proj_catg_type_id: str
    """Project Code"""
    proj_id: str
    """Project"""

    def __eq__(self, __o: "PROJPCAT") -> bool:
        return self.proj_catg_id == __o.proj_catg_id and self.proj_catg_type_id == __o.proj_catg_type_id and self.proj_id == __o.proj_id

    def __hash__(self) -> int:
        return hash((self.proj_catg_id, self.proj_catg_type_id, self.proj_id))

def _process_projpcat_data(projpcat_df: pd.DataFrame) -> dict[str, PROJPCAT]:
    projpcat_dict = {}
    for _, row in projpcat_df.iterrows():
        projpcat = PROJPCAT(
            proj_catg_id=row["proj_catg_id"],
            proj_catg_type_id=row["proj_catg_type_id"],
            proj_id=row["proj_id"]
        )
        projpcat_dict[f"{projpcat.proj_catg_id}_{projpcat.proj_catg_type_id}_{projpcat.proj_id}"] = projpcat
    return projpcat_dict