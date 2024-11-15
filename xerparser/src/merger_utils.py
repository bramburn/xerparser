from typing import List, Dict, Set, Tuple
from .errors import XERMergeError
from datetime import datetime

def detect_conflicts(files: List[Dict]) -> Dict:
    """
    Detect conflicts between multiple XER files.
    
    Returns:
        Dict containing conflicts found in different sections
    """
    conflicts = {
        "PROJECT": _detect_project_conflicts(files),
        "CALENDAR": _detect_calendar_conflicts(files),
        "WBS": _detect_wbs_conflicts(files),
        "TASK": _detect_activity_conflicts(files),
        "RELATIONSHIPS": _detect_relationship_conflicts(files),
        "RESOURCES": _detect_resource_conflicts(files),
    }
    return conflicts

def _detect_project_conflicts(files: List[Dict]) -> List[Dict]:
    """
    Detect conflicts in project metadata.
    
    Checks for conflicts in:
    - project_id
    - project settings
    - dates
    """
    conflicts = []
    project_fields = [
        "proj_id",
        "proj_short_name",
        "proj_name",
        "plan_start_date",
        "plan_end_date",
        "last_recalc_date",
        "current_data_date",
        "def_complete_pct_type",
        "def_duration_type",
        "critical_path_type",
        "def_calendar_id"
    ]
    
    for field in project_fields:
        values = {i: files[i]["PROJECT"].get(field) for i in range(len(files))}
        if len(set(values.values())) > 1:
            conflicts.append({
                "field": field,
                "values": values,
                "type": "project_field_mismatch"
            })
    
    return conflicts

def _detect_calendar_conflicts(files: List[Dict]) -> List[Dict]:
    """
    Detect conflicts in calendars.
    
    Checks for:
    - Calendar ID conflicts
    - Calendar settings conflicts
    - Working hours conflicts
    """
    conflicts = []
    calendar_fields = [
        "clndr_id",
        "clndr_name",
        "day_hr_cnt",
        "week_hr_cnt",
        "month_hr_cnt",
        "year_hr_cnt"
    ]
    
    # Get all calendar IDs across files
    all_calendar_ids = set()
    for file in files:
        all_calendar_ids.update(file.get("CALENDAR", {}).keys())
    
    for calendar_id in all_calendar_ids:
        calendar_values = {}
        for i, file in enumerate(files):
            if calendar_id in file.get("CALENDAR", {}):
                calendar_values[i] = {
                    field: file["CALENDAR"][calendar_id].get(field)
                    for field in calendar_fields
                }
        
        if len(calendar_values) > 1:
            for field in calendar_fields:
                field_values = {
                    i: data[field]
                    for i, data in calendar_values.items()
                }
                if len(set(field_values.values())) > 1:
                    conflicts.append({
                        "calendar_id": calendar_id,
                        "field": field,
                        "values": field_values,
                        "type": "calendar_field_mismatch"
                    })
    
    return conflicts

def _detect_wbs_conflicts(files: List[Dict]) -> List[Dict]:
    """
    Detect conflicts in WBS structures.
    
    Checks for:
    - WBS ID conflicts
    - WBS hierarchy conflicts
    - WBS attribute conflicts
    """
    conflicts = []
    wbs_fields = [
        "wbs_id",
        "proj_id",
        "obs_id",
        "seq_num",
        "parent_wbs_id",
        "wbs_short_name",
        "wbs_name",
        "wbs_level"
    ]
    
    # Get all WBS IDs across files
    all_wbs_ids = set()
    for file in files:
        all_wbs_ids.update(file.get("WBS", {}).keys())
    
    for wbs_id in all_wbs_ids:
        wbs_values = {}
        for i, file in enumerate(files):
            if wbs_id in file.get("WBS", {}):
                wbs_values[i] = {
                    field: file["WBS"][wbs_id].get(field)
                    for field in wbs_fields
                }
        
        if len(wbs_values) > 1:
            # Check hierarchy conflicts
            parent_ids = {
                i: data["parent_wbs_id"]
                for i, data in wbs_values.items()
            }
            if len(set(parent_ids.values())) > 1:
                conflicts.append({
                    "wbs_id": wbs_id,
                    "type": "wbs_hierarchy_conflict",
                    "parent_ids": parent_ids
                })
            
            # Check other field conflicts
            for field in wbs_fields:
                field_values = {
                    i: data[field]
                    for i, data in wbs_values.items()
                }
                if len(set(field_values.values())) > 1:
                    conflicts.append({
                        "wbs_id": wbs_id,
                        "field": field,
                        "values": field_values,
                        "type": "wbs_field_mismatch"
                    })
    
    return conflicts

def _detect_activity_conflicts(files: List[Dict]) -> List[Dict]:
    """
    Detect conflicts in activities.
    
    Checks for:
    - Activity ID conflicts
    - Duration conflicts
    - Date conflicts
    - WBS assignment conflicts
    """
    conflicts = []
    activity_fields = [
        "task_id",
        "proj_id",
        "wbs_id",
        "clndr_id",
        "task_type",
        "task_name",
        "task_code",
        "orig_dur",
        "rem_dur",
        "target_start_date",
        "target_end_date",
        "act_start_date",
        "act_end_date",
        "float_path",
        "total_float"
    ]
    
    # Get all activity IDs across files
    all_activity_ids = set()
    for file in files:
        all_activity_ids.update(file.get("TASK", {}).keys())
    
    for activity_id in all_activity_ids:
        activity_values = {}
        for i, file in enumerate(files):
            if activity_id in file.get("TASK", {}):
                activity_values[i] = {
                    field: file["TASK"][activity_id].get(field)
                    for field in activity_fields
                }
        
        if len(activity_values) > 1:
            for field in activity_fields:
                field_values = {
                    i: data[field]
                    for i, data in activity_values.items()
                }
                if len(set(field_values.values())) > 1:
                    conflicts.append({
                        "activity_id": activity_id,
                        "field": field,
                        "values": field_values,
                        "type": "activity_field_mismatch"
                    })
    
    return conflicts

def _detect_relationship_conflicts(files: List[Dict]) -> List[Dict]:
    """
    Detect conflicts in relationships.
    
    Checks for:
    - Relationship type conflicts
    - Lag conflicts
    - Missing predecessor/successor conflicts
    """
    conflicts = []
    relationship_fields = [
        "rel_id",
        "pred_task_id",
        "succ_task_id",
        "rel_type",
        "lag_hr_cnt",
        "float_path"
    ]
    
    # Get all relationship IDs across files
    all_rel_ids = set()
    for file in files:
        all_rel_ids.update(file.get("RELATIONSHIPS", {}).keys())
    
    for rel_id in all_rel_ids:
        rel_values = {}
        for i, file in enumerate(files):
            if rel_id in file.get("RELATIONSHIPS", {}):
                rel_values[i] = {
                    field: file["RELATIONSHIPS"][rel_id].get(field)
                    for field in relationship_fields
                }
        
        if len(rel_values) > 1:
            for field in relationship_fields:
                field_values = {
                    i: data[field]
                    for i, data in rel_values.items()
                }
                if len(set(field_values.values())) > 1:
                    conflicts.append({
                        "relationship_id": rel_id,
                        "field": field,
                        "values": field_values,
                        "type": "relationship_field_mismatch"
                    })
    
    return conflicts

def _detect_resource_conflicts(files: List[Dict]) -> List[Dict]:
    """
    Detect conflicts in resources.
    
    Checks for:
    - Resource ID conflicts
    - Resource type conflicts
    - Resource assignment conflicts
    """
    conflicts = []
    resource_fields = [
        "rsrc_id",
        "rsrc_name",
        "rsrc_short_name",
        "rsrc_type",
        "parent_rsrc_id",
        "clndr_id",
        "cost_qty_type",
        "unit_type"
    ]
    
    # Get all resource IDs across files
    all_resource_ids = set()
    for file in files:
        all_resource_ids.update(file.get("RESOURCES", {}).keys())
    
    for rsrc_id in all_resource_ids:
        rsrc_values = {}
        for i, file in enumerate(files):
            if rsrc_id in file.get("RESOURCES", {}):
                rsrc_values[i] = {
                    field: file["RESOURCES"][rsrc_id].get(field)
                    for field in resource_fields
                }
        
        if len(rsrc_values) > 1:
            for field in resource_fields:
                field_values = {
                    i: data[field]
                    for i, data in rsrc_values.items()
                }
                if len(set(field_values.values())) > 1:
                    conflicts.append({
                        "resource_id": rsrc_id,
                        "field": field,
                        "values": field_values,
                        "type": "resource_field_mismatch"
                    })
    
    return conflicts

def resolve_conflicts(files: List[Dict], conflicts: Dict, strategy: str) -> Dict:
    """
    Resolve conflicts using the specified strategy.
    
    Args:
        files: List of parsed XER file data
        conflicts: Dictionary of detected conflicts
        strategy: Conflict resolution strategy ("first_wins", "last_wins", "manual")
    
    Returns:
        Dict containing resolved data
    """
    if strategy not in ["first_wins", "last_wins", "manual"]:
        raise XERMergeError(f"Unknown conflict resolution strategy: {strategy}")
    
    resolved_data = {}
    for section in conflicts:
        resolved_data[section] = _resolve_section_conflicts(
            files, conflicts[section], strategy
        )
    
    return resolved_data

def _resolve_section_conflicts(
    files: List[Dict],
    conflicts: List[Dict],
    strategy: str
) -> Dict:
    """
    Resolve conflicts for a specific section based on the chosen strategy.
    
    Args:
        files: List of parsed XER file data
        conflicts: List of conflicts for the section
        strategy: Conflict resolution strategy
    
    Returns:
        Dict containing resolved data for the section
    """
    if strategy == "first_wins":
        return files[0]
    elif strategy == "last_wins":
        return files[-1]
    else:  # manual strategy
        raise XERMergeError("Manual conflict resolution not implemented")