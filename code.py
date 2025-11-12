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
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        current_table = None
        
        for row in reader:
            table_name = row['Table Name'].strip()
            
            # If table name is empty, use the current table
            if table_name:
                current_table = table_name
                if current_table not in tables:
                    tables[current_table] = []
            
            if current_table:
                tables[current_table].append({
                    'column': row['Column'].strip(),
                    'datatype': row['Datatype'].strip(),
                    'required': row['Required'].strip().upper() == 'Y',
                    'description': row['Description'].strip()
                })
    
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
        drop_statement = f"DROP TABLE IF EXISTS `{project_id}.{dataset}.{table_name}`;\n"
        sql_statements.append(drop_statement)
        
        # CREATE TABLE
        create_statement = f"CREATE TABLE `{project_id}.{dataset}.{table_name}` (\n"
        
        column_definitions = []
        for col in columns:
            bq_type = map_datatype(col['datatype'], datatype_mappings)
            nullable = "NOT NULL" if col['required'] else ""
            
            # Add description as comment if available
            options = f"OPTIONS(description=\"{col['description']}\")" if col['description'] else ""
            
            col_def = f"  {col['column']} {bq_type} {nullable} {options}".strip()
            column_definitions.append(col_def)
        
        create_statement += ",\n".join(column_definitions)
        create_statement += "\n);\n"
        
        sql_statements.append(create_statement)
    
    return "\n".join(sql_statements)

def main():
    if len(sys.argv) != 2:
        print("Usage: python bigquery_table_gen.py <path_to_csv_file>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    
    if not os.path.exists(csv_file):
        print(f"Error: File '{csv_file}' not found")
        sys.exit(1)
    
    # Load configuration
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset = os.getenv('BIGQUERY_DATASET')
    target_dir = os.getenv('TARGET_DIRECTORY', './output')
    settings_file = os.getenv('SETTINGS_FILE', 'datatype_mappings.json')
    
    if not project_id or not dataset:
        print("Error: BIGQUERY_PROJECT_ID and BIGQUERY_DATASET must be set in .env file")
        sys.exit(1)
    
    # Load datatype mappings
    datatype_mappings = load_settings(settings_file)
    
    # Parse CSV
    tables = parse_csv(csv_file)
    
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
