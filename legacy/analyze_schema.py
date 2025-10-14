"""
Schema Generator from JSON Data
Analyzes your actual JSON files to generate the exact Baserow table schemas needed
"""

import json
import os
from typing import Dict, List, Set, Any
from datetime import datetime

def analyze_json_structure(json_dir: str = "JSON") -> Dict[str, Dict]:
    """Analyze JSON files to extract field schemas"""
    
    if not os.path.exists(json_dir):
        print(f"Error: {json_dir} directory not found")
        return {}
    
    schemas = {}
    
    json_files = [f for f in os.listdir(json_dir) if f.endswith('.json')]
    
    for filename in json_files:
        filepath = os.path.join(json_dir, filename)
        table_name = filename.replace('.json', '').replace('_data', '').replace('-', '_')
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list) or not data:
                continue
            
            # Analyze all records to get complete field picture
            fields = {}
            sample_size = min(50, len(data))  # Analyze up to 50 records
            
            for record in data[:sample_size]:
                for key, value in record.items():
                    # Skip metadata and relationship fields
                    if (key in ['Id', 'CreatedAt', 'UpdatedAt'] or 
                        key.startswith('_nc_m2m_') or
                        isinstance(value, (list, dict)) and key.startswith('_')):
                        continue
                    
                    # Determine field type
                    field_type = infer_field_type(key, value, record)
                    
                    if key not in fields:
                        fields[key] = {
                            'name': key,
                            'type': field_type,
                            'examples': [],
                            'null_count': 0,
                            'total_count': 0
                        }
                    
                    fields[key]['total_count'] += 1
                    
                    if value is None:
                        fields[key]['null_count'] += 1
                    else:
                        if len(fields[key]['examples']) < 3:
                            fields[key]['examples'].append(str(value)[:50])
            
            # Convert to schema format
            schema_fields = []
            for field_name, field_info in fields.items():
                required = field_info['null_count'] / field_info['total_count'] < 0.5  # Less than 50% nulls
                
                field_config = {
                    'name': field_name,
                    'type': field_info['type'],
                    'required': required
                }
                
                # Add type-specific configurations
                if field_info['type'] == 'number':
                    field_config['number_decimal_places'] = 2
                elif field_info['type'] == 'text' and any(len(ex) > 100 for ex in field_info['examples']):
                    field_config['type'] = 'long_text'
                
                schema_fields.append(field_config)
            
            schemas[table_name] = {
                'filename': filename,
                'record_count': len(data),
                'fields': schema_fields
            }
            
        except Exception as e:
            print(f"Error analyzing {filename}: {e}")
    
    return schemas

def infer_field_type(field_name: str, value: Any, record: Dict) -> str:
    """Infer Baserow field type from value and context"""
    
    if value is None:
        return 'text'  # Default type
    
    # Date fields
    if ('date' in field_name.lower() or 'established' in field_name.lower() or 
        field_name.lower() in ['createdat', 'updatedat']):
        return 'date'
    
    # Check if value looks like a date
    if isinstance(value, str) and len(value) == 10 and value.count('-') == 2:
        try:
            datetime.strptime(value, '%Y-%m-%d')
            return 'date'
        except:
            pass
    
    # Number fields
    if isinstance(value, (int, float)):
        return 'number'
    
    if isinstance(value, str):
        # Try to parse as number
        try:
            float(value.replace(',', ''))
            return 'number'
        except:
            pass
        
        # Long text for longer strings
        if len(value) > 200:
            return 'long_text'
    
    # Boolean fields
    if isinstance(value, bool):
        return 'boolean'
    
    # Email fields
    if 'email' in field_name.lower() and isinstance(value, str):
        return 'email'
    
    # URL fields  
    if ('url' in field_name.lower() or 'link' in field_name.lower()) and isinstance(value, str):
        return 'url'
    
    # Default to text
    return 'text'

def print_schema_analysis(schemas: Dict[str, Dict]):
    """Print detailed schema analysis"""
    
    print("="*80)
    print("JSON DATA SCHEMA ANALYSIS")
    print("="*80)
    
    total_records = 0
    
    for table_name, schema in schemas.items():
        total_records += schema['record_count']
        
        print(f"\n📋 {table_name.upper()}")
        print(f"   File: {schema['filename']}")
        print(f"   Records: {schema['record_count']:,}")
        print(f"   Fields: {len(schema['fields'])}")
        
        # Group fields by type
        field_types = {}
        for field in schema['fields']:
            field_type = field['type']
            if field_type not in field_types:
                field_types[field_type] = []
            field_types[field_type].append(field['name'])
        
        print("   Field Types:")
        for field_type, field_names in field_types.items():
            print(f"     {field_type}: {len(field_names)} ({', '.join(field_names[:3])}{'...' if len(field_names) > 3 else ''})")
    
    print(f"\n📊 SUMMARY: {len(schemas)} tables, {total_records:,} total records")

def generate_creation_script(schemas: Dict[str, Dict]):
    """Generate Python code for table creation"""
    
    print("\n" + "="*80)
    print("GENERATED TABLE CREATION CODE")
    print("="*80)
    
    print("\n# Add this to create_tables.py - get_table_schemas() function:")
    print("def get_table_schemas() -> Dict[str, Dict]:")
    print('    """Define the schema for all tables based on actual JSON data analysis"""')
    print("    ")
    print("    schemas = {")
    
    # Dependency order
    order = [
        "Location", "Role", "Source",  # Foundation
        "People", "Entity",            # Core entities
        "Infrastructure", "Licenses", "Ecosystem",  # Infrastructure
        "Transactions", "Actions_timeline",         # Transactions
        "Discursive_oil", "Related_events", "Memory"  # Communications
    ]
    
    # Generate schema for each table in order
    for table_name in order:
        # Find matching schema
        schema = None
        for schema_name, schema_data in schemas.items():
            if (schema_name.lower().replace('_', '').replace('-', '') == 
                table_name.lower().replace('_', '').replace('-', '')):
                schema = schema_data
                break
        
        if not schema:
            print(f"        # {table_name}: Schema not found in JSON files")
            continue
        
        print(f'        "{table_name}": {{')
        print('            "fields": [')
        
        for field in schema['fields']:
            field_line = f'                {{"name": "{field["name"]}", "type": "{field["type"]}"'
            
            if field.get('required'):
                field_line += ', "required": True'
            
            if field['type'] == 'number' and 'number_decimal_places' in field:
                field_line += f', "number_decimal_places": {field["number_decimal_places"]}'
            
            field_line += '},'
            print(field_line)
        
        print('            ]')
        print('        },')
        print()
    
    print("    }")
    print("    return schemas")

def main():
    """Analyze JSON files and generate schemas"""
    
    print("🔍 Analyzing JSON files to generate Baserow table schemas...")
    
    schemas = analyze_json_structure()
    
    if not schemas:
        print("No JSON files found to analyze.")
        return
    
    # Print analysis
    print_schema_analysis(schemas)
    
    # Generate creation code
    generate_creation_script(schemas)
    
    print(f"\n{'='*80}")
    print("NEXT STEPS:")
    print("1. Review the generated schema above")
    print("2. Copy the schema code to create_tables.py")
    print("3. Run: python create_tables.py")
    print("4. Then run: python putData.py to import data")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()