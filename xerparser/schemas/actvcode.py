# xerparser
# actvcode.py

import pandas as pd
# xerparser
# actvtype.py

import pandas as pd


class ACTVCODE:
    def __init__(self, row: pd.Series) -> None:
        self.actv_code_id: str = row['actv_code_id']
        self.actv_code_type_id: str = row['actv_code_type_id']
        self.actv_code: str = row['actv_code']
        self.actv_code_desc: str = row['actv_code_desc']
        self.is_active: bool = row['is_active']
        self.max_length: int = row['actv_short_len']
        self.proj_id: str = row['proj_id']
        self.scope: str = _check_scope(row['actv_code_type_scope'])
        self.seq_num: int | None = None if pd.isna(row['seq_num']) else int(row['seq_num'])


def _check_scope(value: str) -> str:
    if not value.startswith("AS_"):
        raise ValueError(f"Expected 'AS_Project' or 'AS_Global', got {value}")
    return value[3:]


def _process_actvcode_data(actvcode_df: pd.DataFrame) -> dict[str, ACTVCODE]:
    actvcode_values = {}
    for _, row in actvcode_df.iterrows():
        actvcode = ACTVCODE(row)
        actvcode_values[actvcode.actv_code_id] = actvcode
    return actvcode_values
