#!/usr/bin/env python3
"""
Auto-generate README Configuration Documentation from CSV Rules

This script reads the CSV rule files in processors/rules/ and automatically
updates the README.md "Connector Configurations" section with field documentation.

Usage:
    python generate_readme_docs.py [--dry-run]
"""

import csv
import os
import re
import shutil
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class ConfigField:
    """Represents a configuration field from CSV rules."""
    name: str
    description: str
    required: bool
    default: str
    field_type: str
    valid_values: str
    subsection: str
    importance: str


class ReadmeDocGenerator:
    """Generates README documentation from CSV configuration rules."""
    
    def __init__(self, project_root: str = "."):
        self.project_root = Path(project_root)
        self.rules_dir = self.project_root / "processors" / "rules"
        self.readme_path = self.project_root / "README.md"
        
    def parse_csv_file(self, csv_file_path: Path) -> List[ConfigField]:
        """Parse a CSV rule file and return list of ConfigField objects."""
        fields = []
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Skip empty rows
                    if not row.get('name', '').strip():
                        continue
                        
                    # Get description - ONLY include fields with display_definition
                    description = row.get('display_definition', '').strip()
                    if not description:
                        # Skip fields without display_definition
                        continue
                    
                    what_to_do = row.get('what_do_do', '').strip()
                    if not what_to_do:
                        what_to_do = row.get('what to do', '').strip()
                    
                    # Filter based on validation rules
                    if not self._should_include_field(what_to_do):
                        continue
                    
                    field = ConfigField(
                        name=row.get('name', '').strip(),
                        description=description,
                        required=what_to_do == 'REQUIRE',
                        default=row.get('default', '').strip() or 'N/A',
                        field_type=row.get('type', '').strip(),
                        valid_values=row.get('valid_values', '').strip(),
                        subsection=row.get('subsection', '').strip(),
                        importance=row.get('importance', '').strip()
                    )
                    
                    fields.append(field)
                    
        except Exception as e:
            print(f"Error parsing {csv_file_path}: {e}")
            
        return fields
    
    def _should_include_field(self, what_to_do: str) -> bool:
        """Determine if a field should be included in documentation."""
        what_to_do = what_to_do.upper().strip()
        
        # Include REQUIRE fields
        if what_to_do == 'REQUIRE':
            return True
            
        # Include all ALLOW fields EXCEPT "ALLOW DEFAULT"
        if what_to_do.startswith('ALLOW'):
            if what_to_do == 'ALLOW DEFAULT':
                return False  # Exclude ALLOW DEFAULT
            else:
                return True   # Include all other ALLOW variants
            
        # Exclude everything else (IGNORE, DISALLOW)
        return False
    
    def generate_field_table(self, fields: List[ConfigField]) -> str:
        """Generate markdown table for a list of fields."""
        if not fields:
            return "No configurable fields available.\n\n"
            
        # Sort fields by importance and name
        importance_order = {'high': 0, 'medium': 1, 'low': 2}
        fields.sort(key=lambda f: (importance_order.get(f.importance.lower(), 3), f.name))
        
        table = "| Field | Description | Required | Default | Example |\n"
        table += "|-------|-------------|----------|---------|---------|" + "\n"
        
        for field in fields:
            # Format description with proper line breaks and word wrapping
            description = self._format_description(field.description)
            
            # Create example from valid_values or use generic example
            example = self._create_example(field)
            
            required_text = "Yes" if field.required else "No"
            
            # Format field name to allow wrapping
            field_name = f"`{field.name}`"
            if len(field.name) > 25:
                # Break long field names at dots for better readability
                parts = field.name.split('.')
                if len(parts) > 2:
                    # Group parts to avoid too many breaks
                    formatted_parts = []
                    current_part = parts[0]
                    for part in parts[1:]:
                        if len(current_part + '.' + part) > 25:
                            formatted_parts.append(current_part)
                            current_part = part
                        else:
                            current_part += '.' + part
                    formatted_parts.append(current_part)
                    field_name = f"`{'`<br>`'.join(formatted_parts)}`"
            
            # Escape pipe characters in all table content
            escaped_default = field.default.replace('|', '\\|') if field.default else 'N/A'
            escaped_example = example.replace('|', '\\|') if example else 'value'
            
            table += f"| {field_name} | {description} | {required_text} | `{escaped_default}` | `{escaped_example}` |\n"
        
        table += "\n"
        return table
    
    def _format_description(self, description: str) -> str:
        """Format description text for better table display using markdown."""
        if not description:
            return ""
            
        # Replace multiple whitespace with single spaces
        description = re.sub(r'\s+', ' ', description).strip()
        
        # Escape special characters to prevent formatting issues
        description = description.replace('|', '\\|')  # Escape pipes for tables
        description = description.replace('$', '\\$')  # Escape dollar signs for KaTeX
        
        # Convert bullet points to proper markdown format
        description = description.replace(' - ', '<br>- ')
        
        # Add line breaks for very long descriptions (over 150 chars)
        if len(description) > 150:
            # For descriptions with JSON examples, be more careful about splitting
            if '[{' in description and '}]' in description:
                # Contains JSON - split more conservatively
                # Look for major sentence breaks with capital letters
                parts = []
                current = ""
                
                # Split on sentences that end with period followed by space and capital letter
                # but not inside JSON blocks
                i = 0
                in_json = False
                brace_count = 0
                
                while i < len(description):
                    char = description[i]
                    current += char
                    
                    # Track JSON blocks
                    if char == '{':
                        brace_count += 1
                        in_json = True
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            in_json = False
                    
                    # Look for sentence breaks outside JSON
                    if (char == '.' and not in_json and 
                        i + 2 < len(description) and 
                        description[i + 1] == ' ' and 
                        description[i + 2].isupper() and
                        len(current) > 60):
                        parts.append(current.strip())
                        current = ""
                    
                    i += 1
                
                if current:
                    parts.append(current.strip())
                
                if len(parts) > 1:
                    description = '<br><br>'.join(parts)
            else:
                # No JSON - use simple sentence splitting
                sentences = description.split('. ')
                if len(sentences) > 1:
                    lines = []
                    current_line = ""
                    
                    for i, sentence in enumerate(sentences):
                        if len(current_line + sentence) > 120 and current_line:
                            if not current_line.endswith('.'):
                                current_line += '.'
                            lines.append(current_line.strip())
                            current_line = sentence
                        else:
                            if current_line:
                                current_line += ' ' + sentence
                            else:
                                current_line = sentence
                            if i < len(sentences) - 1:
                                current_line += '.'
                    
                    if current_line:
                        lines.append(current_line.strip())
                    
                    if len(lines) > 1:
                        description = '<br><br>'.join(lines)
        
        return description
    
    def _create_example(self, field: ConfigField) -> str:
        """Create an example value for a field."""
        # Use first valid value if available (but skip validation messages)
        if (field.valid_values and field.valid_values != 'N/A' 
            and not field.valid_values.startswith('A string') 
            and not field.valid_values.startswith('[')
            and not field.valid_values.startswith('(')):
            # Parse comma-separated values and take the first one
            values = [v.strip() for v in field.valid_values.split(',')]
            if values and values[0]:
                return values[0]
        
        # Use default if it's not N/A and not too long and not a validation message
        if (field.default and field.default != 'N/A' and len(field.default) < 50 
            and not field.default.startswith('A string') and not field.default.startswith('[')
            and not field.default.startswith('(')):
            return field.default
            
        # Generate generic examples based on field name patterns
        field_lower = field.name.lower()
        
        if 'key' in field_lower:
            return 'your-api-key'
        elif 'secret' in field_lower or 'password' in field_lower:
            return 'your-secret'
        elif 'user' in field_lower:
            return 'dbuser'
        elif 'database' in field_lower:
            return 'orders'
        elif 'collection' in field_lower:
            return 'transactions'
        elif 'topic' in field_lower and 'prefix' in field_lower:
            return 'ecommerce'
        elif 'topic' in field_lower:
            return 'ecommerce.orders'
        elif field.name == 'name':
            return 'my-processor'
        elif field.name == 'connector.class':
            return 'MongoDbAtlasSource'
        elif 'separator' in field_lower:
            return '.'
        elif 'compression' in field_lower:
            return 'gzip'
        elif field.field_type == 'boolean':
            return 'true'
        elif field.field_type == 'int' or field.field_type == 'long':
            return '300000'
        else:
            return 'value'
    
    def load_all_fields(self) -> Dict[str, List[ConfigField]]:
        """Load fields from all CSV files."""
        csv_files = {
            'general': self.rules_dir / 'general_managed_configs.csv',
            'source': self.rules_dir / 'managed_source_configs.csv', 
            'sink': self.rules_dir / 'managed_sink_configs.csv'
        }
        
        all_fields = {}
        
        for section, csv_path in csv_files.items():
            if csv_path.exists():
                fields = self.parse_csv_file(csv_path)
                all_fields[section] = fields
                print(f"Loaded {len(fields)} fields from {section} config")
            else:
                print(f"Warning: {csv_path} not found")
                all_fields[section] = []
        
        # Remove duplicates - if a field appears in general, remove it from source/sink
        general_field_names = {field.name for field in all_fields.get('general', [])}
        
        if 'source' in all_fields:
            all_fields['source'] = [f for f in all_fields['source'] if f.name not in general_field_names]
            
        if 'sink' in all_fields:
            all_fields['sink'] = [f for f in all_fields['sink'] if f.name not in general_field_names]
        
        return all_fields
    
    def generate_documentation(self) -> str:
        """Generate the complete configuration documentation."""
        all_fields = self.load_all_fields()
        
        doc = ""
        
        # General Configurations
        doc += "### General Configurations\n\n"
        doc += "These fields are common to both source and sink connectors:\n\n"
        doc += self.generate_field_table(all_fields['general'])
        
        # Source Configurations  
        doc += "### Source-Specific Configurations\n\n"
        doc += "These fields are specific to source connectors (MongoDB → Kafka):\n\n"
        doc += self.generate_field_table(all_fields['source'])
        
        # Sink Configurations
        doc += "### Sink-Specific Configurations\n\n" 
        doc += "These fields are specific to sink connectors (Kafka → MongoDB):\n\n"
        doc += self.generate_field_table(all_fields['sink'])
        
        return doc
    
    def update_readme(self, dry_run: bool = False) -> bool:
        """Update the README.md file with generated documentation."""
        if not self.readme_path.exists():
            print(f"Error: README.md not found at {self.readme_path}")
            return False
            
        # Read current README content
        try:
            with open(self.readme_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            print(f"Error reading README.md: {e}")
            return False
        
        # Generate new documentation
        new_docs = self.generate_documentation()
        
        # Find the section to replace
        # Look for "### General Configurations" and replace until "## Configuration Validation"
        pattern = r'(### General Configurations\n\n).*?(?=## Configuration Validation)'
        
        if not re.search(pattern, content, re.DOTALL):
            print("Error: Could not find configuration sections in README.md")
            print("Looking for '### General Configurations' section")
            return False
        
        # Replace the section
        new_content = re.sub(pattern, r'\1' + new_docs, content, flags=re.DOTALL)
        
        if dry_run:
            print("=== DRY RUN: Generated Documentation ===")
            print(new_docs)
            print("=== End of Generated Documentation ===")
            return True
        
        # Backup original file
        backup_path = self.readme_path.with_suffix('.md.backup')
        try:
            shutil.copy2(self.readme_path, backup_path)
            print(f"Created backup: {backup_path}")
        except Exception as e:
            print(f"Warning: Could not create backup: {e}")
        
        # Write updated content
        try:
            with open(self.readme_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print("✓ README.md updated successfully")
            return True
        except Exception as e:
            print(f"Error writing README.md: {e}")
            return False


def main():
    """Main function to handle command line arguments."""
    parser = argparse.ArgumentParser(
        description="Auto-generate README configuration documentation from CSV rules",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview generated documentation without modifying README.md'
    )
    
    args = parser.parse_args()
    
    generator = ReadmeDocGenerator()
    
    success = generator.update_readme(dry_run=args.dry_run)
    
    if not success:
        exit(1)


if __name__ == "__main__":
    main()