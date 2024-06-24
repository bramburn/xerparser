# xerparser
# memotype.py

import pandas as pd

class MEMOTYPE:
    """
    A class to represent a notebook topic.
    """

    def __init__(self, memotype_df: pd.DataFrame) -> None:
        self.memotype_df = memotype_df
        self.memotype_dict = self._process_memotype_data(memotype_df)

    @staticmethod
    def _process_memotype_data(memotype_df: pd.DataFrame) -> dict[str, dict]:
        memotype_dict = {}
        for _, row in memotype_df.iterrows():
            memotype = {
                "uid": row["memo_type_id"],
                "topic": row["memo_type"]
            }
            memotype_dict[memotype["uid"]] = memotype
        return memotype_dict

    def get_memotype(self, memotype_id: str) -> dict:
        return self.memotype_dict[memotype_id]

    def __eq__(self, __o: "MEMOTYPE") -> bool:
        if __o is None:
            return False
        return self.memotype_dict[__o.uid]["topic"] == self.memotype_dict[self.uid]["topic"]

    def __gt__(self, __o: "MEMOTYPE") -> bool:
        return self.memotype_dict[__o.uid]["topic"] > self.memotype_dict[self.uid]["topic"]

    def __lt__(self, __o: "MEMOTYPE") -> bool:
        return self.memotype_dict[__o.uid]["topic"] < self.memotype_dict[self.uid]["topic"]

    def __hash__(self) -> int:
        return hash(self.memotype_dict[self.uid]["topic"])

    def __str__(self) -> str:
        return self.memotype_dict[self.uid]["topic"]