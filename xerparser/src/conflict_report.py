from typing import Dict, List, Optional
import json
from datetime import datetime
from pathlib import Path
from .errors import XERReportError

class ConflictReport:
    """Class for generating and managing conflict reports."""
    
    def __init__(self, conflicts: Dict):
        self.conflicts = conflicts
        self.timestamp = datetime.now()
        self.statistics = self._calculate_statistics()
        
    def save(self, output_path: str) -> None:
        """
        Save the conflict report to the specified path.
        
        Args:
            output_path: Path where the report should be saved
        """
        try:
            if output_path.endswith('.md'):
                self._save_markdown(output_path)
            elif output_path.endswith('.json'):
                self._save_json(output_path)
            else:
                raise XERReportError("Unsupported output format. Use .md or .json")
        except Exception as e:
            raise XERReportError(f"Failed to save report: {str(e)}")
            
    def _save_markdown(self, output_path: str) -> None:
        """Generate and save a markdown report."""
        content = self._generate_markdown()
        Path(output_path).write_text(content)
        
    def _save_json(self, output_path: str) -> None:
        """Generate and save a JSON report."""
        content = self._generate_json()
        Path(output_path).write_text(json.dumps(content, indent=2))
        
    def _generate_markdown(self) -> str:
        """Generate markdown formatted report content."""
        lines = [
            "# XER Merge Conflict Report",
            f"Generated: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Summary",
            self._generate_summary(),
            "",
            "## Statistics",
            self._format_statistics(),
            "",
            "## Detailed Conflicts",
            self._generate_conflict_details(),
            "",
            "## Resolution Suggestions",
            self._generate_suggestions()
        ]
        return "\n".join(filter(None, lines))
    
    def _generate_json(self) -> Dict:
        """Generate JSON formatted report content."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "summary": self._generate_summary_dict(),
            "statistics": self.statistics,
            "conflicts": self.conflicts,
            "suggestions": self._generate_suggestions_dict()
        }
    
    def _calculate_statistics(self) -> Dict:
        """Calculate statistics about the conflicts."""
        stats = {
            "total_conflicts": 0,
            "conflicts_by_section": {},
            "conflicts_by_type": {}
        }
        
        for section, section_conflicts in self.conflicts.items():
            section_count = len(section_conflicts)
            stats["total_conflicts"] += section_count
            stats["conflicts_by_section"][section] = section_count
            
            # Count conflict types
            for conflict in section_conflicts:
                conflict_type = conflict.get("type", "unknown")
                stats["conflicts_by_type"][conflict_type] = \
                    stats["conflicts_by_type"].get(conflict_type, 0) + 1
                    
        return stats
    
    def _format_statistics(self) -> str:
        """Format statistics as markdown."""
        lines = [
            f"- Total Conflicts: {self.statistics['total_conflicts']}",
            "",
            "### Conflicts by Section",
            *[f"- {section}: {count}" 
              for section, count in self.statistics['conflicts_by_section'].items()],
            "",
            "### Conflicts by Type",
            *[f"- {type_}: {count}" 
              for type_, count in self.statistics['conflicts_by_type'].items()]
        ]
        return "\n".join(lines)
    
    def _generate_summary(self) -> str:
        """Generate a text summary of conflicts."""
        if not self.conflicts:
            return "No conflicts detected."
            
        lines = []
        for section, section_conflicts in self.conflicts.items():
            if section_conflicts:
                lines.append(f"\n### {section}")
                lines.append(f"- Found {len(section_conflicts)} conflicts")
                conflict_types = set(c.get("type") for c in section_conflicts)
                lines.append("- Types of conflicts:")
                lines.extend(f"  - {conflict_type}" for conflict_type in conflict_types)
                
        return "\n".join(lines)
    
    def _generate_conflict_details(self) -> str:
        """Generate detailed conflict descriptions."""
        if not self.conflicts:
            return "No conflicts to report."
            
        lines = []
        for section, section_conflicts in self.conflicts.items():
            if section_conflicts:
                lines.append(f"\n### {section}")
                for i, conflict in enumerate(section_conflicts, 1):
                    lines.append(f"\n#### Conflict {i}")
                    lines.append(f"- Type: {conflict.get('type', 'Unknown')}")
                    
                    if "field" in conflict:
                        lines.append(f"- Field: {conflict['field']}")
                    
                    if "values" in conflict:
                        lines.append("- Values:")
                        for file_idx, value in conflict["values"].items():
                            lines.append(f"  - File {file_idx + 1}: {value}")
                            
        return "\n".join(lines)
    
    def _generate_suggestions(self) -> str:
        """Generate conflict resolution suggestions."""
        if not self.conflicts:
            return "No conflicts to resolve."
            
        lines = ["### General Suggestions",
                "1. Review all conflicts carefully before merging",
                "2. Consider using 'last_wins' strategy for progress updates",
                "3. Use 'first_wins' strategy for baseline preservation",
                "4. Manual resolution recommended for critical conflicts",
                "",
                "### Specific Suggestions"]
                
        for section, section_conflicts in self.conflicts.items():
            if section_conflicts:
                lines.append(f"\n#### {section}")
                suggestions = self._get_section_suggestions(section, section_conflicts)
                lines.extend(f"- {suggestion}" for suggestion in suggestions)
                
        return "\n".join(lines)
    
    def _get_section_suggestions(self, section: str, conflicts: List[Dict]) -> List[str]:
        """Get specific suggestions for a section's conflicts."""
        suggestions = []
        if section == "PROJECT":
            suggestions.extend([
                "Verify project dates align with schedule requirements",
                "Ensure consistent calendar assignments"
            ])
        elif section == "CALENDAR":
            suggestions.extend([
                "Check for working hours consistency",
                "Verify holiday and exception patterns"
            ])
        elif section == "WBS":
            suggestions.extend([
                "Review WBS hierarchy for structural integrity",
                "Verify WBS codes follow naming conventions"
            ])
        elif section == "TASK":
            suggestions.extend([
                "Check for duplicate activity IDs",
                "Verify logical relationships",
                "Review duration and date conflicts"
            ])
        elif section == "RELATIONSHIPS":
            suggestions.extend([
                "Check for circular logic",
                "Verify relationship types are appropriate",
                "Review lag values for accuracy"
            ])
        return suggestions
    
    def _generate_summary_dict(self) -> Dict:
        """Generate a dictionary summary of conflicts."""
        summary = {}
        for section, section_conflicts in self.conflicts.items():
            summary[section] = {
                "count": len(section_conflicts),
                "types": list(set(c.get("type") for c in section_conflicts))
            }
        return summary
    
    def _generate_suggestions_dict(self) -> Dict:
        """Generate a dictionary of resolution suggestions."""
        suggestions = {
            "general": [
                "Review all conflicts carefully before merging",
                "Consider using 'last_wins' strategy for progress updates",
                "Use 'first_wins' strategy for baseline preservation",
                "Manual resolution recommended for critical conflicts"
            ],
            "specific": {}
        }
        
        for section, section_conflicts in self.conflicts.items():
            if section_conflicts:
                suggestions["specific"][section] = \
                    self._get_section_suggestions(section, section_conflicts)
                    
        return suggestions 