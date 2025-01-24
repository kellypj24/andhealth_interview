import ijson
import json
from pathlib import Path
from typing import Any, Dict, Set
from collections import defaultdict

class JsonDocumentationGenerator:
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.output_path = self.file_path.parent / "json_structure_doc.md"
        self.structure = defaultdict(set)
        self.examples = {}
        self.max_records = 100  # Analyze first 100 records for better coverage

    def generate_documentation(self):
        """Generate markdown documentation of JSON structure"""
        print("Analyzing JSON structure...")
        self._analyze_file()
        
        md_content = [
            "# 340B OPAIS JSON Structure Documentation\n",
            "## Overview\n",
            "This document outlines the structure of the 340B OPAIS JSON data file.\n",
            "## Data Structure\n",
            "The file contains an array of records with the following structure:\n",
            "```json"
        ]
        
        # Convert structure to organized dictionary
        structure_dict = self._convert_structure_to_dict()
        md_content.append(json.dumps(structure_dict, indent=2))
        md_content.append("```\n")
        
        # Add field details section
        md_content.extend([
            "## Field Details\n",
            "| Field | Data Types | Example Values |\n",
            "|-------|------------|----------------|"
        ])
        
        # Add rows for each field
        for field, types in sorted(self.structure.items()):
            examples = self.examples.get(field, set())
            example_str = " | ".join(str(ex) for ex in list(examples)[:3])
            md_content.append(f"| {field} | {', '.join(types)} | {example_str} |")

        # Write to markdown file
        with open(self.output_path, 'w') as f:
            f.write('\n'.join(md_content))

        return f"Documentation generated at {self.output_path}"

    def _analyze_file(self):
        """Analyze the JSON file structure"""
        record_count = 0
        
        with self.file_path.open('rb') as file:
            # Start parsing at the root array level
            parser = ijson.parse(file)
            current_path = []
            
            for prefix, event, value in parser:
                if record_count >= self.max_records:
                    break
                    
                if event == 'map_key':
                    current_path.append(value)
                elif event != 'start_map' and event != 'end_map':
                    if current_path:
                        # Record the type and example for this field
                        field_name = '.'.join(current_path)
                        self.structure[field_name].add(type(value).__name__)
                        
                        # Store example values
                        if field_name not in self.examples:
                            self.examples[field_name] = set()
                        if len(self.examples[field_name]) < 3:  # Store up to 3 examples
                            self.examples[field_name].add(value)
                        
                        current_path.pop()
                    
                if event == 'end_map':
                    if not current_path:  # End of a record
                        record_count += 1

    def _convert_structure_to_dict(self) -> Dict:
        """Convert the flat structure to a nested dictionary"""
        result = {}
        
        for field_path, types in self.structure.items():
            current = result
            parts = field_path.split('.')
            
            # Build nested structure
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            
            # Add field information
            current[parts[-1]] = {
                "types": list(types),
                "examples": list(self.examples.get(field_path, set()))[:3]
            }
        
        return result

def main():
    file_path = "/Users/pjkelly/andhealth_interview/OPA_CE_DAILY_PUBLIC.JSON"
    generator = JsonDocumentationGenerator(file_path)
    result = generator.generate_documentation()
    print(result)

if __name__ == "__main__":
    main()