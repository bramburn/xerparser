# xerparser
# account.py

import pandas as pd
from xerparser.src.validators import optional_int


class ACCOUNT:
    """
    A class to represent a cost account.
    """

    def __init__(self, row: pd.Series) -> None:
        self.acct_id: str = row['acct_id']
        self.acct_short_name: str = row['acct_short_name']
        self.acct_name: str = row['acct_name']
        self.parent_acct_id: str = row['parent_acct_id']
        self.description: str = _check_description(row['acct_descr'])
        self.seq_num: int | None = optional_int(row['acct_seq_num'])

    def __eq__(self, __o: "ACCOUNT") -> bool:
        if __o is None:
            return False
        return self.acct_name == __o.acct_name and self.acct_id == __o.acct_id

    def __hash__(self) -> int:
        return hash((self.acct_name, self.acct_id))


def _check_description(value: str) -> str:
    return (value, "")[value == "" or value == "ï»¿"]


def _process_account_data( account_df: pd.DataFrame) -> dict[str, ACCOUNT]:
    """
    Process the ACCOUNT table data and create a dictionary of ACCOUNT objects.

    Args:
        account_df (pd.DataFrame): DataFrame containing the ACCOUNT table data.

    Returns:
        dict[str, ACCOUNT]: A dictionary of ACCOUNT objects, keyed by the acct_id.
    """
    accounts = {}
    for _, row in account_df.iterrows():
        account = ACCOUNT(row)
        accounts[account.acct_id] = account
    return accounts
