import ijson
from pathlib import Path
from typing import Dict, Set
from collections import defaultdict


class SimplifiedStructureAnalyzer:
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.output_path = self.file_path.parent / "340b_core_structure.md"
        self.structure = defaultdict(
            lambda: {"types": set(), "sample_values": set(), "count": 0}
        )
        self.max_samples = 3
        self.max_records = 100
        self.record_count = 0
        self.max_depth = 2  # Limit nesting depth for arrays
        self.seen_paths = set()

    def analyze(self):
        """Analyze the JSON structure focusing on unique paths"""
        print(f"Analyzing first {self.max_records} records...")
        print("Progress: [", end="", flush=True)

        with self.file_path.open("rb") as file:
            parser = ijson.parse(file)
            current_path = []
            current_depth = defaultdict(int)  # Track depth for each type of array

            try:
                for prefix, event, value in parser:
                    if self.record_count >= self.max_records:
                        print("] 100%")
                        break

                    # Update progress every 10 records
                    if self.record_count > 0 and self.record_count % 10 == 0:
                        progress = (self.record_count / self.max_records) * 100
                        print("#", end="", flush=True)

                    if event == "map_key":
                        # Check if we're entering a known array type
                        if value in ["medicaidNumbers", "npiNumbers"]:
                            current_depth[value] += 1
                            # Skip if we're too deep
                            if current_depth[value] > self.max_depth:
                                continue
                        current_path.append(value)
                    elif event == "end_map":
                        if current_path:
                            last = current_path[-1]
                            if last in ["medicaidNumbers", "npiNumbers"]:
                                current_depth[last] -= 1
                            current_path.pop()
                    elif event not in ("start_map", "start_array", "end_array"):
                        if current_path:
                            # Only process if we're not too deep in any array
                            if all(
                                depth <= self.max_depth
                                for depth in current_depth.values()
                            ):
                                field_path = ".".join(current_path)
                                if field_path not in self.seen_paths:
                                    self._update_field_info(field_path, value)
                                    self.seen_paths.add(field_path)
                            current_path.pop()

                    # Count complete records
                    if (
                        event == "end_map"
                        and len(current_path) == 1
                        and current_path[0] == "coveredEntities"
                    ):
                        self.record_count += 1

            except Exception as e:
                print(f"\nError during parsing: {str(e)}")

            # Ensure we complete the progress bar
            if self.record_count < self.max_records:
                print(f"] {(self.record_count / self.max_records) * 100:.1f}%")

        self._write_markdown_report()

    def _update_field_info(self, field_path: str, value):
        """Update structure information for a field"""
        field_info = self.structure[field_path]
        field_info["types"].add(type(value).__name__)
        field_info["count"] += 1
        if len(field_info["sample_values"]) < self.max_samples:
            field_info["sample_values"].add(str(value))

    def _write_markdown_report(self):
        """Generate simplified Markdown report"""
        sections = [
            "# 340B OPAIS Core Data Structure\n",
            "## Overview\n",
            f"- File: {self.file_path.name}\n- Sample Size: {self.record_count} records\n",
            "## Core Structure\n",
            "The file contains a root object with a 'coveredEntities' array containing individual entity records.\n",
            "### Primary Fields\n",
        ]

        # Group fields by their top-level parent, but limit depth
        field_groups = self._group_fields()

        # Generate sections for each major group
        for group_name, fields in field_groups.items():
            sections.append(f"\n#### {group_name}\n")
            sections.append("| Field | Data Type | Sample Values |")
            sections.append("|-------|-----------|---------------|")

            for field in sorted(fields):
                # Skip deeply nested paths
                path_parts = field.split(".")
                if len(path_parts) > 3:  # Limit display depth
                    continue

                info = self.structure[field]
                types = ", ".join(sorted(info["types"]))
                samples = " | ".join(sorted(info["sample_values"]))[:100]
                sections.append(f"| {field} | {types} | {samples} |")

        # Add suggested table structure
        sections.extend(
            [
                "\n## Suggested Database Structure\n",
                "Based on the analysis, the following table structure is recommended:\n",
                "\n### Main Table\n",
                "```sql",
                "CREATE TABLE covered_entities (",
                "    id SERIAL PRIMARY KEY,",
                "    id340b TEXT,",
                "    name TEXT,",
                "    entity_type TEXT,",
                "    participating BOOLEAN,",
                "    participating_start_date TIMESTAMP,",
                "    grant_number TEXT",
                ");\n```",
                "\n### Related Tables\n",
                "```sql",
                "CREATE TABLE medicaid_numbers (",
                "    id SERIAL PRIMARY KEY,",
                "    covered_entity_id INTEGER REFERENCES covered_entities(id),",
                "    medicaid_number TEXT,",
                "    state TEXT",
                ");",
                "",
                "CREATE TABLE npi_numbers (",
                "    id SERIAL PRIMARY KEY,",
                "    covered_entity_id INTEGER REFERENCES covered_entities(id),",
                "    npi_number TEXT,",
                "    state TEXT",
                ");",
                "",
                "CREATE TABLE addresses (",
                "    id SERIAL PRIMARY KEY,",
                "    covered_entity_id INTEGER REFERENCES covered_entities(id),",
                "    address_type TEXT,  -- billing, shipping, etc",
                "    address_line1 TEXT,",
                "    address_line2 TEXT,",
                "    city TEXT,",
                "    state TEXT,",
                "    zip TEXT",
                ");```\n",
            ]
        )

        with open(self.output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(sections))

        print(f"\nAnalysis written to: {self.output_path}")

    def _group_fields(self) -> Dict[str, Set[str]]:
        """Group fields by their top-level parent"""
        groups = defaultdict(set)

        for field in self.structure.keys():
            parts = field.split(".")
            if len(parts) == 1:
                groups["Core Fields"].add(field)
            else:
                groups[parts[0]].add(field)

        return groups


def main():
    file_path = "/Users/pjkelly/andhealth_interview/OPA_CE_DAILY_PUBLIC.JSON"
    analyzer = SimplifiedStructureAnalyzer(file_path)
    analyzer.analyze()


if __name__ == "__main__":
    main()
