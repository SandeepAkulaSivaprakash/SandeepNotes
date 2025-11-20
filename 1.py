import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_settings(settings_file='datatype_mappings.json'):
    """Load datatype mappings from settings file"""
    with open(settings_file, 'r') as f:
        return json.load(f)

def parse_csv(csv_file):
    """Parse the input CSV and organize by table"""
    tables = {}
    
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        
        # Debug: Print column names to help diagnose issues
        fieldnames = reader.fieldnames
        print(f"CSV Columns found: {fieldnames}")
        
        # Try to find the correct column names (case-insensitive, strip whitespace)
        col_mapping = {}
        for field in fieldnames:
            clean_field = field.strip().lower()
            if 'table' in clean_field and 'name' in clean_field:
                col_mapping['table_name'] = field
            elif clean_field == 'column':
                col_mapping['column'] = field
            elif 'datatype' in clean_field or 'data type' in clean_field:
                col_mapping['datatype'] = field
            elif 'required' in clean_field:
                col_mapping['required'] = field
            elif 'description' in clean_field:
                col_mapping['description'] = field
        
        # Verify all required columns are found
        required_cols = ['table_name', 'column', 'datatype', 'required', 'description']
        missing = [col for col in required_cols if col not in col_mapping]
        if missing:
            print(f"Error: Could not find columns for: {missing}")
            print(f"Available columns: {fieldnames}")
            sys.exit(1)
        
        current_table = None
        
        for row in reader:
            table_name = row[col_mapping['table_name']].strip()
            
            # If table name is empty, use the current table
            if table_name:
                current_table = table_name
                if current_table not in tables:
                    tables[current_table] = []
            
            if current_table:
                column_name = row[col_mapping['column']].strip()
                
                # Check if column already exists in this table (prevent duplicates)
                existing_columns = [col['column'] for col in tables[current_table]]
                if column_name not in existing_columns:
                    # Replace double quotes with single quotes in description
                    description = row[col_mapping['description']].strip().replace('"', "'")
                    
                    tables[current_table].append({
                        'column': column_name,
                        'datatype': row[col_mapping['datatype']].strip(),
                        'required': row[col_mapping['required']].strip().upper() == 'Y',
                        'description': description
                    })
                else:
                    print(f"  Warning: Duplicate column '{column_name}' in table '{current_table}' - skipping")
    
    return tables

def map_datatype(source_type, mappings):
    """Map source datatype to BigQuery datatype"""
    # Convert to lowercase for case-insensitive matching
    source_type_lower = source_type.lower()
    return mappings.get(source_type_lower, 'STRING')  # Default to STRING if not found

def generate_bigquery_sql(tables, datatype_mappings, project_id, dataset):
    """Generate BigQuery CREATE TABLE statements"""
    sql_statements = []
    
    for table_name, columns in tables.items():
        # DROP TABLE IF EXISTS
        drop_statement = f"DROP TABLE IF EXISTS `{project_id}.{dataset}.{table_name}`;"
        sql_statements.append(drop_statement)
        sql_statements.append("")  # Blank line for readability
        
        # CREATE TABLE
        create_statement = f"CREATE TABLE `{project_id}.{dataset}.{table_name}` (\n"
        
        column_definitions = []
        
        # Add file_dt as the first column (audit column)
        column_definitions.append("  file_dt DATE NOT NULL OPTIONS(description='File processing date')")
        
        # Add all regular columns from CSV
        for col in columns:
            bq_type = map_datatype(col['datatype'], datatype_mappings)
            nullable = "NOT NULL" if col['required'] else ""
            
            # Debug output for datatype mapping
            if col['datatype'].lower() not in [k.lower() for k in datatype_mappings.keys()]:
                print(f"  Info: Mapping '{col['datatype']}' -> '{bq_type}'")
            
            # Build column definition
            col_def = f"  {col['column']} {bq_type}"
            if nullable:
                col_def += f" {nullable}"
            
            # Add description as OPTIONS if available
            if col['description']:
                col_def += f" OPTIONS(description=\"{col['description']}\")"
            
            column_definitions.append(col_def)
        
        # Add insert_dt and update_dt as the last columns (audit columns)
        column_definitions.append("  insert_dt TIMESTAMP NOT NULL OPTIONS(description='Record insertion timestamp')")
        column_definitions.append("  update_dt TIMESTAMP OPTIONS(description='Record last update timestamp')")
        
        create_statement += ",\n".join(column_definitions)
        create_statement += "\n);"
        
        sql_statements.append(create_statement)
        sql_statements.append("")  # Blank line between tables
    
    return "\n".join(sql_statements)

def main():
    if len(sys.argv) != 2:
        print("Usage: python bigquery_table_gen.py <path_to_csv_file>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    
    # Normalize path for Windows
    csv_file = os.path.normpath(csv_file)
    
    if not os.path.exists(csv_file):
        print(f"Error: File '{csv_file}' not found")
        sys.exit(1)
    
    # Load configuration
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset = os.getenv('BIGQUERY_DATASET')
    target_dir = os.getenv('TARGET_DIRECTORY', '.\\output')
    settings_file = os.getenv('SETTINGS_FILE', 'datatype_mappings.json')
    
    if not project_id or not dataset:
        print("Error: BIGQUERY_PROJECT_ID and BIGQUERY_DATASET must be set in .env file")
        sys.exit(1)
    
    # Load datatype mappings
    try:
        datatype_mappings = load_settings(settings_file)
        print(f"Loaded {len(datatype_mappings)} datatype mappings from {settings_file}")
    except FileNotFoundError:
        print(f"Error: Settings file '{settings_file}' not found")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in '{settings_file}'")
        sys.exit(1)
    
    # Parse CSV
    tables = parse_csv(csv_file)
    
    if not tables:
        print("Error: No tables found in CSV file")
        sys.exit(1)
    
    print(f"Tables parsed: {list(tables.keys())}")
    for table_name, columns in tables.items():
        print(f"  - {table_name}: {len(columns)} columns")
    
    # Generate SQL
    sql_content = generate_bigquery_sql(tables, datatype_mappings, project_id, dataset)
    
    # Create output directory if it doesn't exist
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(target_dir, f"bigquery_tables_{timestamp}.sql")
    
    # Write SQL file
    with open(output_file, 'w') as f:
        f.write(sql_content)
    
    print(f"✓ SQL file generated successfully: {output_file}")
    print(f"✓ Tables created: {', '.join(tables.keys())}")

if __name__ == "__main__":
    main()
