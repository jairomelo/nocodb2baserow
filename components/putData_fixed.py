"""
Baserow Data Upload Script - FIXED VERSION
Upload JSON data from CamillaDataset to Baserow database using REST API
This version properly maps field names to Baserow field IDs
"""

import requests
import json
import time
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
import dotenv

# Load environment variables from .env file
dotenv.load_dotenv()

# Configuration
BASEROW_BASE_URL = "http://localhost"
DATABASE_ID = 175
API_TOKEN = os.getenv("API_TOKEN")

# Table name to ID mapping from created Baserow tables
TABLE_MAPPINGS = {
    "Location": 703,
    "Role": 704,
    "Source": 705,
    "People": 706,
    "Entity": 707,
    "Infrastructure": 708,
    "Licenses": 709,
    "Ecosystem": 710,
    "Transactions": 711,
    "Actions-timeline": 712,
    "Discursive-oil": 713,
    "Related-events": 714,
    "Memory": 715,
}

# Global mapping to store old ID -> new Baserow ID relationships
ID_MAPPINGS = {}

# Cache for table field mappings (field_name -> field_id)
FIELD_MAPPINGS = {}

def get_auth_headers() -> Dict[str, str]:
    """Get authentication headers for API requests"""
    return {
        'Authorization': f'Token {API_TOKEN}',
        'Content-Type': 'application/json'
    }

def get_table_fields(table_id: int) -> Dict[str, str]:
    """Get field name to field ID mapping for a table"""
    if table_id in FIELD_MAPPINGS:
        return FIELD_MAPPINGS[table_id]
    
    url = f"{BASEROW_BASE_URL}/api/database/fields/table/{table_id}/"
    
    try:
        response = requests.get(url, headers=get_auth_headers())
        response.raise_for_status()
        fields = response.json()
        
        # Create mapping of field name to field ID
        field_mapping = {}
        for field in fields:
            field_name = field['name']
            field_id = f"field_{field['id']}"
            field_mapping[field_name] = field_id
            # Also map lowercase and normalized versions
            field_mapping[field_name.lower()] = field_id
            field_mapping[field_name.lower().replace(' ', '_').replace('-', '_')] = field_id
        
        FIELD_MAPPINGS[table_id] = field_mapping
        return field_mapping
        
    except Exception as e:
        print(f"❌ Error fetching fields for table {table_id}: {e}")
        return {}

def store_id_mapping(table_name: str, old_id: int, new_baserow_record: Dict) -> None:
    """Store mapping from old NocoDB ID to new Baserow ID"""
    if table_name not in ID_MAPPINGS:
        ID_MAPPINGS[table_name] = {}
    ID_MAPPINGS[table_name][old_id] = new_baserow_record['id']

def get_relationship_field_mappings() -> Dict[str, Dict[str, str]]:
    """Get mapping of relationship types to Baserow field IDs"""
    return {
        'Infrastructure': {
            'linked_location': 'field_6882',
            'linked_entities': 'field_6884'
        },
        'Transactions': {
            'linked_entities': 'field_6886', 
            'linked_people': 'field_6888',
            'linked_sources': 'field_6890'
        },
        'Discursive-oil': {
            'linked_author': 'field_6892',
            'linked_recipient': 'field_6894', 
            'linked_sources': 'field_6896'
        }
    }

def clean_field_name(field_name: str) -> str:
    """Normalize field names for consistent mapping"""
    # Handle special mappings first
    special_mappings = {
        'latitude (N)': 'latitude_n',
        'longitude (E)': 'longitude_e',
        'entity_type (past)': 'entity_type_past',
        'activity focus': 'activity_focus',
        'operating locations': 'operating_locations',
        'current status': 'current_status',
        'unique-identifier': 'unique_identifier',
        'type-source': 'type_source',
        'Source_date': 'source_date',
        'Transaction type': 'transaction_type',
        'Date_recorded': 'date_recorded',
        'regulated-activity': 'regulated_activity',
        'start-date': 'start_date',
        'end-date': 'end_date',
        'type-of-action': 'type_of_action',
        'type of source': 'type_of_source',
        'Exploration License': 'exploration_license',
        'established-date': 'established_date'
    }
    
    if field_name in special_mappings:
        return special_mappings[field_name]
    
    # General normalization
    return field_name.lower().replace(' ', '_').replace('-', '_')

def is_date_field(field_name: str) -> bool:
    """Check if a field should be treated as a date"""
    date_indicators = ['date', 'established', 'start', 'end', 'createdat', 'updatedat']
    return any(indicator in field_name.lower() for indicator in date_indicators)

def clean_data_for_baserow(data: Dict[str, Any], table_name: str, table_id: int) -> tuple[Dict[str, Any], Dict[str, List[int]]]:
    """
    Clean and transform data to match Baserow requirements with proper field ID mapping
    Returns: (cleaned_data, relationships_data)
    """
    cleaned = {}
    relationships = {}
    
    # Get field mappings for this table
    field_mapping = get_table_fields(table_id)
    
    # Special field mappings for primary fields
    primary_field_mappings = {
        'Location': 'location',  # map "location" field to "Name" primary field
        'People': 'first_name',  # map first_name to Name
        'Entity': 'name',       # map name to Name  
        'Source': 'source',     # map source to Name
        'Role': 'role'          # map role to Name
    }
    
    # Fields to skip (common NocoDB metadata fields)
    skip_fields = {
        'CreatedAt', 'UpdatedAt', 'Id',  # These might be auto-generated
        # Add count fields that are calculated
        'roles', 'transactions', 'related_events', 'actions-timelines',
        'discursive_oils', 'entities', 'is-part-of', 'Infrastructure',
        'concessions-grantee', 'concessions-granter', 'locations',
        'infrastructures', 'people', 'primary-source', 'author-entity',
        'parties_involved', 'primary-source', 'People Involved',
        'Writings about this', 'concessions', 'action-timeline',
        'infrastructure', 'ecosystem-consequence', 'source',
        'legal basis', 'people-involved', 'entities-involved',
        'related_people', 'discussions-around', 'ecosystem_consequences',
        'location (s)', 'entities', 'licenses', 'related_events',
        'actions- timeline', 'Affiliated-entity', 'location',
        'discursive_oils', 'exploration-drillings', 'exploration-productions',
        'granted_to', 'granted_by', 'convention', 'infrastructure_linked'
    }
    
    for key, value in data.items():
        # Skip metadata fields
        if key in skip_fields:
            continue
        
        # Handle relationship fields (starting with _nc_m2m_)
        if key.startswith('_nc_m2m_'):
            if isinstance(value, list) and len(value) > 0:
                relationships[key] = value
            continue
        
        # Handle object relationships (like author, recipient)
        if isinstance(value, dict) and 'Id' in value:
            # Store the ID for later relationship mapping
            relationships[f"object_{key}"] = [value['Id']]
            continue
        
        # Special handling for primary fields
        if table_name in primary_field_mappings and key == primary_field_mappings[table_name]:
            if 'Name' in field_mapping:
                field_id = field_mapping['Name']
            else:
                print(f"   ⚠️  Primary field 'Name' not found in {table_name}")
                continue
        else:
            # Clean field names and find corresponding field ID
            normalized_key = clean_field_name(key)
            
            # Try different variations to find the field ID
            field_id = None
            for candidate in [key, key.lower(), normalized_key]:
                if candidate in field_mapping:
                    field_id = field_mapping[candidate]
                    break
        
        if not field_id:
            print(f"   ⚠️  Field '{key}' not found in table {table_name}, skipping")
            continue
        
        # Handle None values or empty strings for date fields
        if value is None or value == "":
            if is_date_field(key):
                # Skip empty date fields entirely rather than sending empty strings
                continue
            else:
                cleaned[field_id] = ""
        # Handle dates - validate format
        elif isinstance(value, str) and is_date_field(key):
            # Handle various date formats
            if 'T' in value:  # ISO datetime format
                cleaned[field_id] = value.split('T')[0]
            elif len(value) == 4 and value.isdigit():  # Year only like "1961"
                cleaned[field_id] = f"{value}-01-01"  # Convert to full date
            elif len(value) == 10 and value.count('-') == 2:  # Already in YYYY-MM-DD format
                cleaned[field_id] = value
            else:
                # Skip invalid date formats
                print(f"   ⚠️  Skipping invalid date format '{value}' for field '{key}'")
                continue
        # Handle boolean fields
        elif isinstance(value, bool):
            cleaned[field_id] = value
        # Skip empty lists/dicts that aren't relationships
        elif isinstance(value, (list, dict)) and not value:
            continue
        else:
            cleaned[field_id] = str(value) if value is not None else ""
    
    return cleaned, relationships

def map_relationships_to_baserow(relationships: Dict[str, List], table_name: str) -> Dict[str, List[int]]:
    """Convert NocoDB relationships to Baserow relationship field format"""
    baserow_relationships = {}
    field_mappings = get_relationship_field_mappings().get(table_name, {})
    
    for rel_key, rel_data in relationships.items():
        if not isinstance(rel_data, list) or not rel_data:
            continue
            
        # Handle different relationship types
        if rel_key == '_nc_m2m_infrastructure_locations' and 'linked_location' in field_mappings:
            # Map location IDs
            location_ids = []
            for rel in rel_data:
                old_location_id = rel.get('location_id')
                if old_location_id and 'Location' in ID_MAPPINGS:
                    new_id = ID_MAPPINGS['Location'].get(old_location_id)
                    if new_id:
                        location_ids.append(new_id)
            if location_ids:
                baserow_relationships[field_mappings['linked_location']] = location_ids
                
        elif rel_key == '_nc_m2m_entity_infrastructures' and 'linked_entities' in field_mappings:
            # Map entity IDs
            entity_ids = []
            for rel in rel_data:
                old_entity_id = rel.get('entity_id')
                if old_entity_id and 'Entity' in ID_MAPPINGS:
                    new_id = ID_MAPPINGS['Entity'].get(old_entity_id)
                    if new_id:
                        entity_ids.append(new_id)
            if entity_ids:
                baserow_relationships[field_mappings['linked_entities']] = entity_ids
                
        elif rel_key == '_nc_m2m_transactions_entities' and 'linked_entities' in field_mappings:
            # Map entity IDs for transactions
            entity_ids = []
            for rel in rel_data:
                old_entity_id = rel.get('entity_id')
                if old_entity_id and 'Entity' in ID_MAPPINGS:
                    new_id = ID_MAPPINGS['Entity'].get(old_entity_id)
                    if new_id:
                        entity_ids.append(new_id)
            if entity_ids:
                baserow_relationships[field_mappings['linked_entities']] = entity_ids
                
        elif rel_key == '_nc_m2m_transactions_people' and 'linked_people' in field_mappings:
            # Map people IDs for transactions
            people_ids = []
            for rel in rel_data:
                old_people_id = rel.get('people_id')
                if old_people_id and 'People' in ID_MAPPINGS:
                    new_id = ID_MAPPINGS['People'].get(old_people_id)
                    if new_id:
                        people_ids.append(new_id)
            if people_ids:
                baserow_relationships[field_mappings['linked_people']] = people_ids
                
        elif rel_key == '_nc_m2m_discursive_oil_primary_sources' and 'linked_sources' in field_mappings:
            # Map source IDs
            source_ids = []
            for rel in rel_data:
                old_source_id = rel.get('primary_sources_id')
                if old_source_id and 'Source' in ID_MAPPINGS:
                    new_id = ID_MAPPINGS['Source'].get(old_source_id)
                    if new_id:
                        source_ids.append(new_id)
            if source_ids:
                baserow_relationships[field_mappings['linked_sources']] = source_ids
        
        # Handle object relationships (like author, recipient)
        elif rel_key == 'object_author' and 'linked_author' in field_mappings:
            author_ids = []
            for old_people_id in rel_data:
                if 'People' in ID_MAPPINGS:
                    new_id = ID_MAPPINGS['People'].get(old_people_id)
                    if new_id:
                        author_ids.append(new_id)
            if author_ids:
                baserow_relationships[field_mappings['linked_author']] = author_ids
                
        elif rel_key == 'object_recipient' and 'linked_recipient' in field_mappings:
            recipient_ids = []
            for old_people_id in rel_data:
                if 'People' in ID_MAPPINGS:
                    new_id = ID_MAPPINGS['People'].get(old_people_id)
                    if new_id:
                        recipient_ids.append(new_id)
            if recipient_ids:
                baserow_relationships[field_mappings['linked_recipient']] = recipient_ids
    
    return baserow_relationships

def create_record(table_id: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Create a single record in Baserow table"""
    url = f"{BASEROW_BASE_URL}/api/database/rows/table/{table_id}/"
    
    try:
        response = requests.post(
            url,
            headers=get_auth_headers(),
            json=data
        )
        
        if response.status_code == 200:
            created_record = response.json()
            print(f"✅ Created record with ID: {created_record.get('id')}")
            return created_record
        else:
            print(f"❌ Failed to create record: {response.status_code}")
            print(f"   Response: {response.text}")
            print(f"   Data sent: {json.dumps(data, indent=2)}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request error: {e}")
        return None

def update_record_relationships(table_id: int, record_id: int, relationships: Dict[str, List[int]]) -> bool:
    """Update a record with relationship data"""
    url = f"{BASEROW_BASE_URL}/api/database/rows/table/{table_id}/{record_id}/"
    
    try:
        response = requests.patch(
            url,
            headers=get_auth_headers(),
            json=relationships
        )
        
        if response.status_code == 200:
            print(f"   ✅ Updated relationships for record {record_id}")
            return True
        else:
            print(f"   ⚠️  Failed to update relationships: {response.status_code}")
            print(f"      Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Relationship update error: {e}")
        return False

def clear_table_data(table_id: int):
    """Clear all existing data from a table"""
    print(f"   🗑️  Clearing existing data from table {table_id}...")
    
    # Get all records
    url = f"{BASEROW_BASE_URL}/api/database/rows/table/{table_id}/?size=200"
    
    try:
        response = requests.get(url, headers=get_auth_headers())
        if response.status_code == 200:
            data = response.json()
            records = data.get('results', [])
            
            print(f"   Found {len(records)} existing records to delete")
            
            # Delete each record
            for record in records:
                delete_url = f"{BASEROW_BASE_URL}/api/database/rows/table/{table_id}/{record['id']}/"
                delete_response = requests.delete(delete_url, headers=get_auth_headers())
                if delete_response.status_code == 204:
                    print(f"   ✅ Deleted record {record['id']}")
                else:
                    print(f"   ❌ Failed to delete record {record['id']}: {delete_response.status_code}")
        
        print(f"   ✅ Table {table_id} cleared")
        
    except Exception as e:
        print(f"   ❌ Error clearing table: {e}")

def import_table_data(table_name: str, json_file_path: str):
    """Import data from JSON file to Baserow table with relationship handling"""
    print(f"\n🔄 Processing {table_name}...")
    
    # Get table ID
    table_id = TABLE_MAPPINGS.get(table_name)
    if not table_id:
        print(f"❌ Table '{table_name}' not found in mappings")
        return
    
    # Clear existing data first
    clear_table_data(table_id)
    
    # Load JSON data
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
    except Exception as e:
        print(f"❌ Error loading {json_file_path}: {e}")
        return
    
    # Determine data structure
    if isinstance(json_data, list):
        items = json_data
    elif isinstance(json_data, dict) and 'list' in json_data:
        items = json_data['list']
    else:
        print(f"❌ Unexpected JSON structure in {json_file_path}")
        return
    
    print(f"📊 Found {len(items)} records to import")
    
    # Import each record
    success_count = 0
    error_count = 0
    
    for i, item in enumerate(items, 1):
        try:
            # Clean the data for Baserow and extract relationships
            cleaned_item, relationships_data = clean_data_for_baserow(item, table_name, table_id)
            
            # First, create the record without relationships
            result = create_record(table_id, cleaned_item)
            
            if result:
                # Store ID mapping for future relationship references
                old_id = item.get('Id')
                if old_id:
                    store_id_mapping(table_name, old_id, result)
                
                # Handle relationships if any exist and we have the necessary mappings
                if relationships_data:
                    baserow_relationships = map_relationships_to_baserow(relationships_data, table_name)
                    if baserow_relationships:
                        # Update record with relationships
                        update_record_relationships(table_id, result['id'], baserow_relationships)
                
                success_count += 1
                print(f"   Record {i}/{len(items)} - Success (ID: {result['id']})")
            else:
                error_count += 1
                print(f"   Record {i}/{len(items)} - Failed")
                
        except Exception as e:
            error_count += 1
            print(f"   Record {i}/{len(items)} - Error: {e}")
            import traceback
            traceback.print_exc()
        
        # Optional: Add delay to avoid rate limiting
        time.sleep(0.1)
    
    print(f"\n📈 Import Summary for {table_name}:")
    print(f"   ✅ Success: {success_count}")
    print(f"   ❌ Errors: {error_count}")
    print(f"   📊 Total: {len(items)}")

def main():
    """Main function to upload all JSON files"""
    
    # Check if API token is set
    if not API_TOKEN:
        print("Error: Please set your Baserow API token in the .env file")
        print("You can get your token from: http://localhost/settings/tokens")
        return
    
    print("🚀 Starting Baserow Data Import (FIXED VERSION)...")
    print(f"Base URL: {BASEROW_BASE_URL}")
    print(f"Database ID: {DATABASE_ID}")
    
    # IMPORTANT: Upload files in dependency order to avoid foreign key conflicts
    IMPORT_ORDER = [
        # Phase 1: Independent base tables (no dependencies)
        ("Location_data.json", "Location"),
        ("Role_data.json", "Role"), 
        ("Source_data.json", "Source"),
        
        # Phase 2: Core entity tables
        ("People_data.json", "People"),
        ("Entity_data.json", "Entity"),
        
        # Phase 3: Infrastructure and licensing
        ("Infrastructure_data.json", "Infrastructure"),
        ("Licenses_data.json", "Licenses"),
        ("Ecosystem_data.json", "Ecosystem"),
        
        # Phase 4: Transactional data
        ("Transactions_data.json", "Transactions"),
        ("Actions-timeline_data.json", "Actions-timeline"),
        
        # Phase 5: Communication and events
        ("Discursive-oil_data.json", "Discursive-oil"),
        ("Related-events_data.json", "Related-events"),
        ("Memory_data.json", "Memory")
    ]
    
    json_dir = "JSON"
    
    if not os.path.exists(json_dir):
        print(f"Error: {json_dir} directory not found")
        return
    
    # Upload files in dependency order
    success_count = 0
    total_files = len(IMPORT_ORDER)
    
    print(f"\n{'='*60}")
    print("STARTING FIXED IMPORT PROCESS")
    print("Now properly mapping field names to field IDs")
    print(f"{'='*60}")
    
    for phase_num, (filename, table_name) in enumerate(IMPORT_ORDER, 1):
        print(f"\n--- Phase {phase_num}/{total_files}: {filename} ---")
        
        # Check if file exists
        json_file_path = os.path.join(json_dir, filename)
        if not os.path.exists(json_file_path):
            print(f"⚠️  File not found: {filename} - skipping")
            continue
        
        # Get table ID from mapping
        table_id = TABLE_MAPPINGS.get(table_name)
        if table_id is None:
            print(f"⚠️  No table ID mapping found for {table_name} - skipping")
            continue
        
        print(f"📤 Uploading to table: {table_name} (ID: {table_id})")
        
        # Import the data
        try:
            import_table_data(table_name, json_file_path)
            success_count += 1
            print(f"✅ {filename} processed successfully!")
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")
            import traceback
            traceback.print_exc()
        
        # Pause between files to avoid rate limiting
        if phase_num < total_files:
            print("⏳ Pausing 3 seconds before next file...")
            time.sleep(3)
    
    print(f"\n{'='*60}")
    print("IMPORT SUMMARY")
    print(f"{'='*60}")
    print(f"Files successfully processed: {success_count}/{total_files}")
    print(f"Success rate: {(success_count/total_files*100):.1f}%")
    
    if success_count < total_files:
        print(f"\n⚠️  {total_files - success_count} files failed to process")
        print("Check the error messages above and:")
        print("1. Verify table ID mappings in TABLE_MAPPINGS")
        print("2. Check Baserow field compatibility") 
        print("3. Ensure sufficient API permissions")
    else:
        print("\n🎉 All files processed successfully!")
        print(f"\n📊 ID Mappings created: {len(ID_MAPPINGS)} tables")
        for table_name, mappings in ID_MAPPINGS.items():
            print(f"   {table_name}: {len(mappings)} records")
    
    print(f"{'='*60}")

if __name__ == "__main__":
    main()