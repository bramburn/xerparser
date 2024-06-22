# xerparser
# xer.py

import logging
from itertools import groupby
from pathlib import Path
from typing import Any, BinaryIO

import pandas as pd

from xerparser.schemas.account import _process_account_data
from xerparser.schemas.actvcode import ACTVCODE, _process_actvcode_data
from xerparser.schemas.actvtype import _process_actvtype_data
from xerparser.schemas.calendars import _process_calendar_data
from xerparser.schemas.pcatval import PCATVAL
from xerparser.schemas.project import PROJECT
from xerparser.schemas.projwbs import PROJWBS, _process_projwbs_data
from xerparser.schemas.rsrcrate import RSRCRATE
from xerparser.schemas.task import TASK, LinkToTask
from xerparser.schemas.taskfin import TASKFIN
from xerparser.schemas.taskmemo import TASKMEMO
from xerparser.schemas.taskpred import TASKPRED
from xerparser.schemas.taskrsrc import TASKRSRC
from xerparser.schemas.trsrcfin import TRSRCFIN
from xerparser.schemas.udftype import UDFTYPE
from xerparser.src.errors import CorruptXerFile, find_xer_errors
from xerparser.src.parser import CODEC, file_reader, parser

logging.basicConfig(
    filename='xer_parser.log',
    level=logging.WARNING,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


class Xer:
    """
    A class to represent the schedule data included in a .xer file.
    """

    # class variables
    CODEC = CODEC

    def __init__(self, xer_file_contents: str) -> None:
        self.tables = self._parse_xer_data(xer_file_contents)
        self._process_data()

    def _parse_xer_data(self, xer_file_contents: str) -> dict[str, list]:
        """Parse the XER file contents and return the table data."""
        if errors := find_xer_errors(self.tables):
            raise CorruptXerFile(errors)
        return parser(xer_file_contents)

    def _process_data(self) -> None:

        # todo: add types

        # Set up additional relationships and data
        self._set_proj_activity_codes()
        self._set_proj_codes()
        self._set_proj_calendars()

        self._set_task_actv_codes()
        self._set_task_memos()
        self._set_task_resources()
        self._set_financial_periods()
        self._set_udf_values()

    @classmethod
    def reader(cls, file: Path | str | BinaryIO) -> "Xer":
        """
        Create an Xer object directly from a .XER file.

        Files can be passed as a:
            * Path directory (str or pathlib.Path)
            * Binary file (from requests, Flask, FastAPI, etc...)

        """
        file_contents = file_reader(file)
        return cls(file_contents)

    def _process_dataframe(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        # Preprocess the DataFrame using pandas and NumPy
        if table_name == 'FINDATES':
            df['start_date'] = pd.to_datetime(df['start_date'])
            df['end_date'] = pd.to_datetime(df['end_date'])
        elif table_name == 'ACCOUNT':
            df['acct_id'] = df['acct_id'].astype(str)
            df['parent_acct_id'] = df['parent_acct_id'].astype(str)
            self.accounts = _process_account_data(df)
        elif table_name == 'ACTVTYPE':
            df['actv_code_type_id'] = df['actv_code_type_id'].astype(str)
            df['proj_id'] = df['proj_id'].astype(str)
            df['actv_short_len'] = df['actv_short_len'].astype(int)
            df['seq_num'] = df['seq_num'].fillna(-1).astype(int)
            self.activity_code_types = _process_actvtype_data(df)
        elif table_name == 'ACTVCODE':
            df['actv_code_id'] = df['actv_code_id'].astype(str)
            df['actv_code_type_id'] = df['actv_code_type_id'].astype(str)
            df['parent_actv_code_id'] = df['parent_actv_code_id'].astype(str)
            self.activity_code_values = _process_actvcode_data(df)
        elif table_name == 'CALENDAR':
            df['clndr_id'] = df['clndr_id'].astype(str)
            df['proj_id'] = df['proj_id'].astype(str)
            self.calendars = _process_calendar_data(df)
        elif table_name == 'PROJWBS':
            df['wbs_id'] = df['wbs_id'].astype(str)
            df['proj_id'] = df['proj_id'].astype(str)
            df['parent_wbs_id'] = df['parent_wbs_id'].astype(str)
            self.wbs_nodes = _process_projwbs_data(df)

        # add relationships after
        return df


def proj_key(obj: Any) -> str:
    return (obj.proj_id, "")[obj.proj_id is None]
