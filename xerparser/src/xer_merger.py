from typing import List, Dict, Optional
from .parser import Parser
from .xer_writer import XerWriter
from .validators import SchemaValidator
from .errors import XERMergeError
from .merger_utils import detect_conflicts, resolve_conflicts
from .conflict_report import ConflictReport

class XerMerger:
    """Main class for handling XER file merging operations."""
    
    def __init__(self):
        self.files: List[Dict] = []
        self.parser = Parser()
        self.writer = XerWriter()
        self.validator = SchemaValidator()
        
    def add_file(self, file_path: str) -> None:
        """Add an XER file to the merger."""
        parsed_data = self.parser.parse(file_path)
        self.validator.validate(parsed_data)
        self.files.append(parsed_data)
        
    def generate_conflict_report(self, output_path: str) -> None:
        """Generate a conflict report for the added files."""
        if len(self.files) < 2:
            raise XERMergeError("At least two files are required for conflict detection")
        
        conflicts = detect_conflicts(self.files)
        report = ConflictReport(conflicts)
        report.save(output_path)
        
    def merge(self, strategy: str = "first_wins") -> Dict:
        """Merge the added XER files using the specified strategy."""
        if len(self.files) < 2:
            raise XERMergeError("At least two files are required for merging")
            
        conflicts = detect_conflicts(self.files)
        resolved_data = resolve_conflicts(self.files, conflicts, strategy)
        
        return self._merge_sections(resolved_data)
    
    def write_merged_file(self, output_path: str, merged_data: Dict) -> None:
        """Write the merged data to an XER file."""
        self.writer.write(merged_data, output_path)
        
    def _merge_sections(self, resolved_data: Dict) -> Dict:
        """Merge different sections of the XER files."""
        merged_data = {
            "PROJECT": self._merge_projects(resolved_data.get("PROJECT", {})),
            "CALENDAR": self._merge_calendars(resolved_data.get("CALENDAR", {})),
            "WBS": self._merge_wbs(resolved_data.get("WBS", {})),
            "TASK": self._merge_activities(resolved_data.get("TASK", {})),
            "RELATIONSHIPS": self._merge_relationships(resolved_data.get("RELATIONSHIPS", {})),
            "RESOURCES": self._merge_resources(resolved_data.get("RESOURCES", {}))
        }
        return merged_data
    
    def _merge_projects(self, data: Dict) -> Dict:
        """
        Merge project sections.
        
        Args:
            data: Dictionary containing resolved project data
            
        Returns:
            Dict containing merged project information
        """
        merged_project = {
            "proj_id": data.get("proj_id", ""),
            "proj_short_name": data.get("proj_short_name", ""),
            "proj_name": data.get("proj_name", "Merged Project"),
            "plan_start_date": data.get("plan_start_date", ""),
            "plan_end_date": data.get("plan_end_date", ""),
            "last_recalc_date": data.get("last_recalc_date", ""),
            "current_data_date": data.get("current_data_date", ""),
            "def_complete_pct_type": data.get("def_complete_pct_type", ""),
            "def_duration_type": data.get("def_duration_type", ""),
            "critical_path_type": data.get("critical_path_type", ""),
            "def_calendar_id": data.get("def_calendar_id", "")
        }
        return merged_project
    
    def _merge_calendars(self, data: Dict) -> Dict:
        """
        Merge calendar sections.
        
        Args:
            data: Dictionary containing resolved calendar data
            
        Returns:
            Dict containing merged calendar information
        """
        merged_calendars = {}
        for calendar_id, calendar_data in data.items():
            merged_calendars[calendar_id] = {
                "clndr_id": calendar_id,
                "clndr_name": calendar_data.get("clndr_name", ""),
                "day_hr_cnt": calendar_data.get("day_hr_cnt", 8.0),
                "week_hr_cnt": calendar_data.get("week_hr_cnt", 40.0),
                "month_hr_cnt": calendar_data.get("month_hr_cnt", 172.0),
                "year_hr_cnt": calendar_data.get("year_hr_cnt", 2000.0),
                "clndr_data": calendar_data.get("clndr_data", [])
            }
        return merged_calendars
    
    def _merge_wbs(self, data: Dict) -> Dict:
        """
        Merge WBS sections.
        
        Args:
            data: Dictionary containing resolved WBS data
            
        Returns:
            Dict containing merged WBS structure
        """
        merged_wbs = {}
        for wbs_id, wbs_data in data.items():
            merged_wbs[wbs_id] = {
                "wbs_id": wbs_id,
                "proj_id": wbs_data.get("proj_id", ""),
                "obs_id": wbs_data.get("obs_id", ""),
                "seq_num": wbs_data.get("seq_num", 0),
                "parent_wbs_id": wbs_data.get("parent_wbs_id", ""),
                "wbs_short_name": wbs_data.get("wbs_short_name", ""),
                "wbs_name": wbs_data.get("wbs_name", ""),
                "wbs_level": wbs_data.get("wbs_level", 1)
            }
        return merged_wbs
    
    def _merge_activities(self, data: Dict) -> Dict:
        """
        Merge activity sections.
        
        Args:
            data: Dictionary containing resolved activity data
            
        Returns:
            Dict containing merged activities
        """
        merged_activities = {}
        for task_id, task_data in data.items():
            merged_activities[task_id] = {
                "task_id": task_id,
                "proj_id": task_data.get("proj_id", ""),
                "wbs_id": task_data.get("wbs_id", ""),
                "clndr_id": task_data.get("clndr_id", ""),
                "task_type": task_data.get("task_type", ""),
                "task_name": task_data.get("task_name", ""),
                "task_code": task_data.get("task_code", ""),
                "orig_dur": task_data.get("orig_dur", 0),
                "rem_dur": task_data.get("rem_dur", 0),
                "target_start_date": task_data.get("target_start_date", ""),
                "target_end_date": task_data.get("target_end_date", ""),
                "act_start_date": task_data.get("act_start_date", ""),
                "act_end_date": task_data.get("act_end_date", ""),
                "float_path": task_data.get("float_path", 0),
                "total_float": task_data.get("total_float", 0)
            }
        return merged_activities
    
    def _merge_relationships(self, data: Dict) -> Dict:
        """
        Merge relationship sections.
        
        Args:
            data: Dictionary containing resolved relationship data
            
        Returns:
            Dict containing merged relationships
        """
        merged_relationships = {}
        for rel_id, rel_data in data.items():
            merged_relationships[rel_id] = {
                "rel_id": rel_id,
                "pred_task_id": rel_data.get("pred_task_id", ""),
                "succ_task_id": rel_data.get("succ_task_id", ""),
                "rel_type": rel_data.get("rel_type", ""),
                "lag_hr_cnt": rel_data.get("lag_hr_cnt", 0),
                "float_path": rel_data.get("float_path", 0)
            }
        return merged_relationships
    
    def _merge_resources(self, data: Dict) -> Dict:
        """
        Merge resource sections.
        
        Args:
            data: Dictionary containing resolved resource data
            
        Returns:
            Dict containing merged resources
        """
        merged_resources = {}
        for rsrc_id, rsrc_data in data.items():
            merged_resources[rsrc_id] = {
                "rsrc_id": rsrc_id,
                "rsrc_name": rsrc_data.get("rsrc_name", ""),
                "rsrc_short_name": rsrc_data.get("rsrc_short_name", ""),
                "rsrc_type": rsrc_data.get("rsrc_type", ""),
                "parent_rsrc_id": rsrc_data.get("parent_rsrc_id", ""),
                "clndr_id": rsrc_data.get("clndr_id", ""),
                "cost_qty_type": rsrc_data.get("cost_qty_type", ""),
                "unit_type": rsrc_data.get("unit_type", "")
            }
        return merged_resources 