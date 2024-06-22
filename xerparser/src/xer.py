# xerparser
# xer.py

from itertools import groupby
from pathlib import Path
from typing import Any, BinaryIO

import pandas as pd

from xerparser.schemas import TABLE_UID_MAP
from xerparser.schemas._node import build_tree
from xerparser.schemas.account import ACCOUNT, _process_account_data
from xerparser.schemas.actvcode import ACTVCODE
from xerparser.schemas.actvtype import ACTVTYPE, _process_actvcode_data, _process_actvtype_data
from xerparser.schemas.calendars import CALENDAR, _process_calendar_data
from xerparser.schemas.ermhdr import ERMHDR
from xerparser.schemas.findates import FINDATES
from xerparser.schemas.memotype import MEMOTYPE
from xerparser.schemas.pcattype import PCATTYPE
from xerparser.schemas.pcatval import PCATVAL
from xerparser.schemas.project import PROJECT
from xerparser.schemas.projwbs import PROJWBS
from xerparser.schemas.rsrc import RSRC
from xerparser.schemas.rsrcrate import RSRCRATE
from xerparser.schemas.schedoptions import SCHEDOPTIONS
from xerparser.schemas.task import TASK, LinkToTask
from xerparser.schemas.taskfin import TASKFIN
from xerparser.schemas.taskmemo import TASKMEMO
from xerparser.schemas.taskpred import TASKPRED
from xerparser.schemas.taskrsrc import TASKRSRC
from xerparser.schemas.trsrcfin import TRSRCFIN
from xerparser.schemas.udftype import UDFTYPE
from xerparser.src.errors import CorruptXerFile, find_xer_errors
from xerparser.src.parser import CODEC, file_reader, parser
import logging

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

        # First pass: Process ACTVTYPE data
        for table_name, table_data in self.tables.items():
            if table_name == 'ACTVTYPE':
                self.activity_code_types = self._process_actvtype_data(pd.DataFrame(table_data))

        # Second pass: Process ACTVCODE data
        for table_name, table_data in self.tables.items():
            if table_name == 'ACTVCODE':
                self.activity_code_values = self._process_actvcode_data(pd.DataFrame(table_data),
                                                                        self.activity_code_types)
            elif table_name == 'CALENDAR':
                self.calendars = self._process_calendar_data(pd.DataFrame(table_data), self.projects, self.resources)

        # Process other tables
        for table_name, table_data in self.tables.items():
            if table_name == 'ACCOUNT':
                self.accounts = self._process_account_data(pd.DataFrame(table_data))
            elif table_name == 'CALENDAR':
                self.calendars = self._process_calendar_data(pd.DataFrame(table_data))
            elif table_name == 'FINDATES':
                self.financial_periods = self._process_findates_data(pd.DataFrame(table_data))
            elif table_name == 'MEMOTYPE':
                self.notebook_topics = self._process_memotype_data(pd.DataFrame(table_data))
            elif table_name == 'PCATTYPE':
                self.project_code_types = self._process_pcattype_data(pd.DataFrame(table_data))
            elif table_name == 'PCATVAL':
                self.project_code_values = self._process_pcatval_data(pd.DataFrame(table_data), self.project_code_types)
            elif table_name == 'PROJECT':
                self.projects = self._process_project_data(pd.DataFrame(table_data), self.calendars, self.sched_options)
            elif table_name == 'RSRC':
                self.resources = self._process_rsrc_data(pd.DataFrame(table_data))
            elif table_name == 'RSRCRATE':
                self.resource_rates = self._process_rsrcrate_data(pd.DataFrame(table_data), self.resources)
            elif table_name == 'SCHEDOPTIONS':
                self.sched_options = self._process_schedoptions_data(pd.DataFrame(table_data))
            elif table_name == 'TASK':
                self.tasks = self._process_task_data(pd.DataFrame(table_data), self.calendars, self.wbs_nodes)
            elif table_name == 'TASKPRED':
                self.relationships = self._process_taskpred_data(pd.DataFrame(table_data), self.tasks)
            elif table_name == 'PROJWBS':
                self.wbs_nodes = self._process_projwbs_data(pd.DataFrame(table_data), self.projects)
            elif table_name == 'UDFTYPE':
                self.udf_types = self._process_udftype_data(pd.DataFrame(table_data))
            elif table_name == 'TASKRSRC':
                self._process_taskrsrc_data(pd.DataFrame(table_data), self.resources, self.tasks, self.projects)
            elif table_name == 'TASKFIN':
                self._process_taskfin_data(pd.DataFrame(table_data), self.tasks, self.financial_periods)
            elif table_name == 'TRSRCFIN':
                self._process_trsrcfin_data(pd.DataFrame(table_data), self.tasks, self.financial_periods)

        # Set up additional relationships and data
        self._set_proj_activity_codes()
        self._set_proj_codes()
    #    self._set_proj_calendars() do not set relationships

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

    def _get_activity_codes(self) -> dict[str, ACTVCODE]:
        activity_code_values = {
            code_val["actv_code_id"]: ACTVCODE(
                code_type=self.activity_code_types[code_val["actv_code_type_id"]],
                **code_val,
            )
            for code_val in self.tables.get("ACTVCODE", [])
        }
        return activity_code_values

    def _get_attr(self, table_name: str) -> pd.DataFrame:
        if table := self.tables.get(table_name):
            # Convert the list of dictionaries to a DataFrame
            df = pd.DataFrame(table)

            # Add any DataFrame-specific processing here
            # For example, converting columns to the correct data types
            df = self._process_dataframe(df, table_name)

            return df
        return pd.DataFrame()

    def _get_projects(self) -> dict[str, PROJECT]:
        projects = {
            proj["proj_id"]: PROJECT(
                self.sched_options[proj["proj_id"]],
                self.calendars.get(proj["clndr_id"]),
                **proj,
            )
            for proj in self.tables.get("PROJECT", [])
            if proj["export_flag"] == "Y"
        }
        return projects

    def _get_proj_codes(self) -> dict[str, PCATVAL]:
        project_code_values = {
            code_val["proj_catg_id"]: PCATVAL(
                code_type=self.project_code_types[code_val["proj_catg_type_id"]],
                **code_val,
            )
            for code_val in self.tables.get("PCATVAL", [])
        }

        return project_code_values

    def _get_relationships(self) -> dict[str, TASKPRED]:
        return {
            rel["task_pred_id"]: self._set_taskpred(**rel)
            for rel in self.tables.get("TASKPRED", [])
        }

    def _get_rsrc_rates(self) -> dict[str, RSRCRATE]:
        return {
            rr["rsrc_rate_id"]: self._set_rsrc_rates(**rr)
            for rr in self.tables.get("RSRCRATE", [])
        }

    def _get_tasks(self) -> dict[str, TASK]:
        return {
            task["task_id"]: self._set_task(**task)
            for task in self.tables.get("TASK", [])
        }

    def _get_wbs_nodes(self) -> dict[str, PROJWBS]:
        nodes: dict[str, PROJWBS] = self._get_attr("PROJWBS")
        for node in nodes.values():
            node.parent = nodes.get(node.parent_id)
            if node.parent:
                node.parent.addChild(node)
            if proj := self.projects.get(node.proj_id):
                proj.wbs_nodes.append(node)
                if node.is_proj_node:
                    proj.wbs_root = node
        return nodes

    def _set_proj_activity_codes(self) -> None:
        code_group = groupby(
            sorted(self.activity_code_types.values(), key=proj_key), proj_key
        )
        for proj_id, codes in code_group:
            if proj := self.projects.get(proj_id):
                proj.activity_codes = list(codes)

    def _set_proj_calendars(self) -> None:
        for project in self.projects.values():
            cal_list = []
            for cal in self.calendars.values():
                if not cal.proj_id or cal.proj_id == project.uid:
                    cal_list.append(cal)
            project.calendars = cal_list

    def _set_proj_codes(self) -> None:
        for proj_code in self.tables.get("PROJPCAT", []):
            proj = self.projects.get(proj_code["proj_id"])
            code = self.project_code_values.get(proj_code["proj_catg_id"])
            if proj and code:
                proj.project_codes.update({code.code_type: code})

    def _set_task_actv_codes(self) -> None:
        for act_code in self.tables.get("TASKACTV", []):
            task = self.tasks.get(act_code["task_id"])
            code_value = self.activity_code_values.get(act_code["actv_code_id"])
            if task and code_value:
                task.activity_codes.update({code_value.code_type: code_value})

    def _set_task_memos(self) -> None:
        for memo in self.tables.get("TASKMEMO", []):
            self._set_memo(**memo)

    def _set_task_resources(self) -> None:
        for res in self.tables.get("TASKRSRC", []):
            self._set_taskrsrc(**res)

    def _set_udf_values(self) -> None:
        for udf in self.tables.get("UDFVALUE", []):
            udf_type = self.udf_types[udf["udf_type_id"]]
            udf_value = UDFTYPE.get_udf_value(udf_type, **udf)
            if udf_type.table == "TASK":
                self.tasks[udf["fk_id"]].user_defined_fields[udf_type] = udf_value
            elif udf_type.table == "PROJECT":
                self.projects[udf["fk_id"]].user_defined_fields[udf_type] = udf_value
            elif udf_type.table == "PROJWBS":
                self.wbs_nodes[udf["fk_id"]].user_defined_fields[udf_type] = udf_value
            elif udf_type.table == "RSRC":
                self.resources[udf["fk_id"]].user_defined_fields[udf_type] = udf_value

    def _set_financial_periods(self) -> None:
        for task_fin in self.tables.get("TASKFIN", []):
            self._set_taskfin(**task_fin)

        for rsrc_fin in self.tables.get("TRSRCFIN", []):
            self._set_taskrsrc_fin(**rsrc_fin)

    def _set_memo(self, **kwargs) -> None:
        topic = self.notebook_topics[kwargs["memo_type_id"]].topic
        task = self.tasks[kwargs["task_id"]]
        task.memos.append(TASKMEMO(topic=topic, **kwargs))

    def _set_rsrc_rates(self, **kwargs) -> RSRCRATE:
        rsrc = self.resources[kwargs["rsrc_id"]]
        rsrc_rate = RSRCRATE(rsrc, **kwargs)
        return rsrc_rate

    def _set_task(self, **kwargs) -> TASK:
        calendar = self.calendars[kwargs["clndr_id"]]
        wbs = self.wbs_nodes[kwargs["wbs_id"]]
        wbs.assignments += 1
        task = TASK(calendar=calendar, wbs=wbs, **kwargs)
        self.projects[task.proj_id].tasks.append(task)
        return task

    def _set_taskpred(self, **kwargs) -> TASKPRED:
        pred = self.tasks[kwargs["pred_task_id"]]
        succ = self.tasks[kwargs["task_id"]]
        task_pred = TASKPRED(predecessor=pred, successor=succ, **kwargs)
        pred.successors.append(LinkToTask(succ, task_pred.link, task_pred.lag))
        succ.predecessors.append(LinkToTask(pred, task_pred.link, task_pred.lag))
        self.projects[task_pred.proj_id].relationships.append(task_pred)
        return task_pred

    def _set_taskrsrc(self, **kwargs) -> None:
        rsrc = self.resources[kwargs["rsrc_id"]]
        account = self.accounts.get(kwargs["acct_id"])
        task = self.tasks[kwargs["task_id"]]
        proj = self.projects[kwargs["proj_id"]]
        taskrsrc = TASKRSRC(resource=rsrc, account=account, **kwargs)
        task.resources.update({taskrsrc.uid: taskrsrc})
        proj.resources.append(taskrsrc)

    def _set_taskfin(self, **kwargs) -> None:
        period = self.financial_periods[kwargs["fin_dates_id"]]
        task_fin = TASKFIN(period=period, **kwargs)
        self.tasks[task_fin.task_id].periods.append(task_fin)

    def _set_taskrsrc_fin(self, **kwargs) -> None:
        period = self.financial_periods[kwargs["fin_dates_id"]]
        rsrc_fin = TRSRCFIN(period=period, **kwargs)
        self.tasks[rsrc_fin.task_id].resources[rsrc_fin.taskrsrc_id].periods.append(
            rsrc_fin
        )

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

        # add relationships after
        return df


def proj_key(obj: Any) -> str:
    return (obj.proj_id, "")[obj.proj_id is None]
