# actvtype.py
import pandas as pd

class ACTVTYPE:
    """
    A class representing Activity Codes Types
    """

    def __init__(self, row: pd.Series) -> None:
        self.uid: str = row['actv_code_type_id']
        """Unique Table ID"""
        self.max_length: int = int(row['actv_short_len'])
        """Max Character Length of Acticity Code"""
        self.name: str = row['actv_code_type']
        """Name of Activity Code Type"""
        self.proj_id: str = row['proj_id']
        """Foreign Key for Project (Project Level Activity Codes)"""
        self.scope: str = _check_scope(row['actv_code_type_scope'])
        """Activity Code Scope - Global, Enterpise, or Project"""
        self.seq_num: int | None = None if pd.isna(row['seq_num']) else int(row['seq_num'])
        """Sort Order"""

    def __eq__(self, __o: "ACTVTYPE") -> bool:
        return all(
            (
                self.max_length == __o.max_length,
                self.name == __o.name,
                self.scope == __o.scope,
            )
        )

    def __gt__(self, __o: "ACTVTYPE") -> bool:
        return self.name > __o.name

    def __lt__(self, __o: "ACTVTYPE") -> bool:
        return self.name < __o.name

    def __hash__(self) -> int:
        return hash((self.max_length, self.name, self.scope))

def _check_scope(value: str) -> str:
    if not value.startswith("AS_"):
        raise ValueError(f"Expected 'AS_Project' or 'AS_Global', got {value}")
    return value[3:]

def _process_actvtype_data( actvtype_df: pd.DataFrame) -> dict[str, ACTVTYPE]:
    actvtype_dict = {}
    for _, row in actvtype_df.iterrows():
        actvtype = ACTVTYPE(row)
        actvtype_dict[actvtype.uid] = actvtype
    return actvtype_dict