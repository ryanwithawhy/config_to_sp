#!/usr/bin/env python3
"""
Script to add a CSV source column to the parameter mapping file.
"""

import csv
import os

def load_csv_parameters(csv_path):
    """Load parameter names from a CSV file."""
    params = set()
    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get('name', '').strip()
                if name:
                    params.add(name)
    return params

def main():
    # Paths
    general_csv = 'processors/rules/general_managed_configs.csv'
    source_csv = 'processors/rules/managed_source_configs.csv'
    sink_csv = 'processors/rules/managed_sink_configs.csv'
    mapping_csv = 'local_resources/parameter_mapping.csv'
    output_csv = 'local_resources/parameter_mapping_with_source.csv'
    
    # Load parameters from each CSV
    general_params = load_csv_parameters(general_csv)
    source_params = load_csv_parameters(source_csv)
    sink_params = load_csv_parameters(sink_csv)
    
    print(f"General CSV has {len(general_params)} parameters")
    print(f"Source CSV has {len(source_params)} parameters") 
    print(f"Sink CSV has {len(sink_params)} parameters")
    
    # Debug: print some parameters from each file
    print(f"\nFirst 5 General params: {list(general_params)[:5]}")
    print(f"First 5 Source params: {list(source_params)[:5]}")
    print(f"First 5 Sink params: {list(sink_params)[:5]}")
    
    # Read the parameter mapping file and add source column
    rows = []
    param_names = []
    
    with open(mapping_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = [f for f in reader.fieldnames if f is not None] + ['CSV Source']
        
        for row in reader:
            param_name = row['Parameter Name'].strip()
            param_names.append(param_name)
            
            # Check which CSVs contain this parameter
            sources = []
            if param_name in general_params:
                sources.append('General')
            if param_name in source_params:
                sources.append('Source')
            if param_name in sink_params:
                sources.append('Sink')
            
            if sources:
                csv_source = ', '.join(sources)
            else:
                csv_source = 'Not Found'
            
            print(f"Parameter '{param_name}' found in: {csv_source}")
            
            # Clean row of None keys and add CSV Source
            clean_row = {k: v for k, v in row.items() if k is not None}
            clean_row['CSV Source'] = csv_source
            rows.append(clean_row)
    
    print(f"\nProcessed {len(param_names)} parameters from mapping file")
    
    # Write the updated file
    with open(output_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Updated parameter mapping saved to {output_csv}")
    
    # Print summary
    source_counts = {}
    for row in rows:
        source = row['CSV Source']
        source_counts[source] = source_counts.get(source, 0) + 1
    
    print("\nSource distribution:")
    for source, count in source_counts.items():
        print(f"  {source}: {count}")

if __name__ == '__main__':
    main()