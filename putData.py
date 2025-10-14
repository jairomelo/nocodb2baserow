"""
Comprehensive CamillaDataset Migration Script
Migrates JSON data from NocoDB to Baserow using modular components

This script orchestrates the complete migration process:
1. Loads table schemas and creates field mappings
2. Imports data in dependency order to handle relationships
3. Maps NocoDB relationships to Baserow link fields
4. Provides detailed logging and error handling

Usage:
    python putData.py [--dry-run] [--clear] [--table TABLE_NAME]
"""

import os
import json
import time
import argparse
from typing import Dict, List, Optional, Any
import dotenv

# Import our custom components
from components.baserow_client import BaserowClient
from components.data_transformer import DataTransformer

# Import required classes for schema management
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class FieldInfo:
    id: int
    name: str
    type: str
    primary: bool = False
    required: bool = False
    linked_table_id: Optional[int] = None

@dataclass
class TableSchema:
    table_id: int
    table_name: str
    fields: List[FieldInfo]
    primary_field: Optional[FieldInfo] = None
    
    def __post_init__(self):
        # Auto-detect primary field
        for field in self.fields:
            if field.primary:
                self.primary_field = field
                break
    
    @property
    def field_name_to_id(self) -> Dict[str, str]:
        """Map field names to Baserow field IDs"""
        return {field.name: f"field_{field.id}" for field in self.fields}
    
    def get_field_by_name(self, name: str) -> Optional[FieldInfo]:
        """Find field by name (case-insensitive)"""
        for field in self.fields:
            if field.name.lower() == name.lower():
                return field
        return None

class SchemaAnalyzer:
    """Analyze and manage table schemas"""
    
    def __init__(self, client: BaserowClient):
        self.client = client
        self._schema_cache = {}
    
    def get_table_schema(self, table_id: int, table_name: Optional[str] = None) -> TableSchema:
        """Get comprehensive table schema"""
        if table_id in self._schema_cache:
            return self._schema_cache[table_id]
        
        fields_data = self.client.get_table_fields(table_id)
        fields = [
            FieldInfo(
                id=field['id'],
                name=field['name'],
                type=field['type'],
                primary=field.get('primary', False),
                required=field.get('required', False),
                linked_table_id=field.get('link_row_table_id')
            )
            for field in fields_data
        ]
        
        schema = TableSchema(
            table_id=table_id,
            table_name=table_name or f"table_{table_id}",
            fields=fields
        )
        
        self._schema_cache[table_id] = schema
        return schema

# Load environment variables
dotenv.load_dotenv()

class CamillaMigrationManager:
    """Main migration orchestrator for CamillaDataset"""
    
    def __init__(self):
        # Configuration from environment
        self.base_url = os.getenv("BASEROW_BASE_URL", "http://localhost")
        self.database_id = int(os.getenv("DATABASE_ID", 175))
        self.api_token = os.getenv("API_TOKEN")
        
        if not self.api_token:
            raise ValueError("API_TOKEN must be set in .env file")
        
        # Get JWT token for structural operations
        self.jwt_token = None
        self.user_email = os.getenv("USER_EMAIL")
        self.user_password = os.getenv("USER_PASSWORD")
        
        # Initialize components
        self.client = BaserowClient(self.base_url, self.api_token, rate_limit_delay=0.1)
        
        # Get JWT token if credentials are available
        if self.user_email and self.user_password:
            self.jwt_token = self.client.get_jwt_token(self.user_email, self.user_password)
            if not self.jwt_token:
                print("⚠️  Could not obtain JWT token. Some operations may fail.")
        else:
            print("⚠️  USER_EMAIL and USER_PASSWORD not found in .env file")
            print("   Some operations requiring JWT authentication may fail.")
        
        self.schema_analyzer = SchemaAnalyzer(self.client)
        self.transformer = DataTransformer()
        
        # Data structures for tracking migration
        self.id_mappings = {}  # old_id -> new_id mappings per table
        self.table_schemas = {}  # table_name -> TableSchema
        self.migration_stats = {}  # table_name -> {success: int, errors: int}
        
        # Table mappings - will be populated dynamically
        self.table_mappings = {}
        
        # Expected table names (should match what create_tables.py generates)
        self.expected_tables = [
            "Location", "Role", "Source", "People", "Entity", 
            "Infrastructure", "Licenses", "Ecosystem", "Transactions", 
            "Actions-timeline", "Discursive-oil", "Related-events", "Memory"
        ]
        
        # Import order to handle dependencies
        self.import_order = [
            # Phase 1: Foundation tables (no dependencies)
            ("Location_data.json", "Location"),
            ("Role_data.json", "Role"),
            ("Source_data.json", "Source"),
            
            # Phase 2: Core entities
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
    
    def discover_tables(self):
        """Discover actual table IDs from the database"""
        print("🔍 Discovering tables in database...")
        
        try:
            # Get all tables in the database
            tables = self.client.get_database_tables(self.database_id)
            
            # Map table names to IDs
            found_tables = {}
            for table in tables:
                table_name = table['name']
                table_id = table['id']
                found_tables[table_name] = table_id
                print(f"  📋 Found table: {table_name} (ID: {table_id})")
            
            # Check which expected tables exist (handle naming variations)
            missing_tables = []
            for expected_table in self.expected_tables:
                found = False
                
                # Try exact match first
                if expected_table in found_tables:
                    self.table_mappings[expected_table] = found_tables[expected_table]
                    found = True
                else:
                    # Try various naming conventions
                    variations = [
                        expected_table.replace('-', '_'),  # Actions-timeline -> Actions_timeline
                        expected_table.replace('-', '_').title().replace('_', '_'),  # Actions_Timeline
                        expected_table.title().replace('-', '_'),  # Actions_Timeline
                    ]
                    
                    for variation in variations:
                        if variation in found_tables:
                            self.table_mappings[expected_table] = found_tables[variation]
                            found = True
                            break
                
                if not found:
                    missing_tables.append(expected_table)
            
            if missing_tables:
                print(f"\n⚠️  Missing tables: {', '.join(missing_tables)}")
                print("💡 Please run create_tables.py first to create the required tables")
                return False
            
            print(f"✅ Found all {len(self.expected_tables)} required tables")
            return True
            
        except Exception as e:
            print(f"❌ Error discovering tables: {e}")
            return False
    
    def initialize_schemas(self):
        """Load and analyze all table schemas"""
        print("🔍 Analyzing Baserow table schemas...")
        
        for table_name, table_id in self.table_mappings.items():
            try:
                schema = self.schema_analyzer.get_table_schema(table_id, table_name)
                self.table_schemas[table_name] = schema
                print(f"  ✅ {table_name}: {len(schema.fields)} fields")
            except Exception as e:
                print(f"  ❌ {table_name}: {e}")
                raise
        
        print(f"✅ Loaded {len(self.table_schemas)} table schemas")
    
    def create_field_mapping(self, table_name: str) -> Dict[str, str]:
        """Create mapping from JSON field names to Baserow field IDs"""
        schema = self.table_schemas.get(table_name)
        if not schema:
            return {}
        
        # Load table-specific field mappings
        field_mappings = {
            # Location mappings
            "Location": {
                "location": "Name",  # Primary field
                "notes": "notes",
                "latitude (N)": "latitude_n",
                "longitude (E)": "longitude_e", 
                "admin_level_country": "admin_level_country"
            },
            
            # People mappings
            "People": {
                "first_name": "Name",  # Primary field (maps to Name in Baserow)
                "last_name1": "last_name1",
                "notes": "notes",
                "discursive_oil_id": "discursive_oil_id",
                "discursive_oil_id1": "discursive_oil_id1",
                "attachment": "attachment",
                "discursive_oil": "discursive_oil",
                "discursive_oil_copy": "discursive_oil_copy"
            },
            
            # Entity mappings
            "Entity": {
                "name": "Name",  # Primary field
                "operating_locations": "operating_locations",
                "entity_national_affiliation": "entity_national_affiliation",
                "descriptive_name": "descriptive_name",
                "entity_type (past)": "entity_type_past",
                "established-date": "established_date",
                "activity focus": "activity_focus",
                "notes": "notes",
                "current status": "current_status",
                "Attachment": "attachment"
            },
            
            # Role mappings
            "Role": {
                "role": "Name",  # Primary field
                "title": "title",
                "description": "description",
                "department": "department",
                "subdepartment": "subdepartment",
                "notes": "notes",
                "start_date": "start_date",
                "end_date": "end_date"
            },
            
            # Source mappings
            "Source": {
                "title": "Name",  # Primary field
                "unique-identifier": "unique_identifier",
                "NB": "nb",
                "Source_date": "source_date",
                "author": "author",
                "type-source": "type_source"
            },
            
            # Infrastructure mappings
            "Infrastructure": {
                "infrastructure_name": "Name",  # Primary field
                "infrastructure_type": "infrastructure_type",
                "notes": "notes",
                "Attachment": "attachment",
                "status": "status"
            },
            
            # Licenses mappings
            "Licenses": {
                "start-date": "start_date",
                "geographic_scope": "geographic_scope",
                "Exploration License": "exploration_license"
            },
            
            # Ecosystem mappings
            "Ecosystem": {
                "title": "Name",  # Primary field
                "consequence_type": "consequence_type",
                "consequence_positive_negative": "consequence_positive_negative",
                "consequence_communities": "consequence_communities",
                "notes": "notes"
            },
            
            # Transactions mappings
            "Transactions": {
                "Title": "Name",  # Primary field
                "Transaction type": "transaction_type",
                "Date_recorded": "date_recorded",
                "notes": "notes",
                "regulated-activity": "regulated_activity"
            },
            
            # Actions-timeline mappings
            "Actions-timeline": {
                "title": "Name",  # Primary field
                "start-date": "start_date",
                "end-date": "end_date",
                "product": "product",
                "type-of-action": "type_of_action"
            },
            
            # Discursive-oil mappings
            "Discursive-oil": {
                "Title": "Name",  # Primary field
                "communication_date": "communication_date",
                "related_feeling": "related_feeling",
                "notes": "notes",
                "obsidian_reference": "obsidian_reference",
                "type of source": "type_of_source",
                "author": "author",
                "recipient": "recipient"
            },
            
            # Related-events mappings
            "Related-events": {
                "event_title": "Name",  # Primary field
                "event_date_start": "event_date_start",
                "event_date_end": "event_date_end",
                "event_type": "event_type",
                "source_obsidian": "source_obsidian",
                "notes": "notes"
            },
            
            # Memory mappings
            "Memory": {
                "memory_title": "Name",  # Primary field
                "memory_type": "memory_type",
                "date_recorded": "date_recorded",
                "description": "description",
                "notes": "notes"
            }
        }
        
        # Get the base mapping for this table
        base_mapping = field_mappings.get(table_name, {})
        
        # Convert to field IDs
        field_id_mapping = {}
        for json_field, baserow_field_name in base_mapping.items():
            field_info = schema.get_field_by_name(baserow_field_name)
            if field_info:
                field_id_mapping[json_field] = f"field_{field_info.id}"
            else:
                print(f"  ⚠️  Field '{baserow_field_name}' not found in {table_name}")
        
        return field_id_mapping
    
    def transform_record_data(self, record: Dict[str, Any], field_mapping: Dict[str, str], 
                             table_name: str) -> Dict[str, Any]:
        """Transform a record's core data (excluding relationships)"""
        cleaned_data = {}
        
        # Skip only essential metadata fields - relationships are handled separately
        skip_fields = {
            'CreatedAt', 'UpdatedAt', 'Id'  # NocoDB metadata that shouldn't be migrated
        }
        
        for json_field, value in record.items():
            # Skip relationships and metadata fields
            if (json_field.startswith('_nc_m2m_') or 
                json_field in skip_fields or
                isinstance(value, dict) and 'Id' in value):
                continue
            
            # Check if we have a mapping for this field
            if json_field in field_mapping:
                field_id = field_mapping[json_field]
                
                # Get field info to validate type compatibility
                schema = self.table_schemas.get(table_name)
                if schema:
                    # Extract field ID number for lookup
                    field_id_number = int(field_id.replace('field_', ''))
                    field_info = next((f for f in schema.fields if f.id == field_id_number), None)
                    
                    # Skip if trying to send non-relationship data to link fields
                    if field_info and field_info.type == 'link_row':
                        continue  # Skip - relationships are handled separately
                
            else:
                # Try automatic mapping for unmapped fields
                schema = self.table_schemas.get(table_name)
                if schema:
                    field_info = schema.get_field_by_name(json_field)
                    if field_info:
                        # Skip link fields from automatic mapping
                        if field_info.type == 'link_row':
                            continue
                        field_id = f"field_{field_info.id}"
                    else:
                        # Skip numeric reference fields that look like NocoDB metadata
                        if (isinstance(value, (int, str)) and 
                            str(value).isdigit() and 
                            any(ref_word in json_field.lower() for ref_word in ['people', 'role', 'entity', 'source', 'infrastructure', 'location'])):
                            continue
                        continue  # Skip unmapped fields
                else:
                    continue
            
            # Transform the value
            transformed_value = self._transform_value(value, json_field)
            if transformed_value is not None:
                cleaned_data[field_id] = transformed_value
        
        return cleaned_data
    
    def _transform_value(self, value: Any, field_name: str) -> Any:
        """Transform a single value based on its type and field name"""
        if value is None or value == "":
            return None
        
        # Date field handling
        if self._is_date_field(field_name):
            return self._normalize_date(value)
        
        # Boolean handling
        if isinstance(value, bool):
            return value
        
        # Convert everything else to string
        return str(value)
    
    def _is_date_field(self, field_name: str) -> bool:
        """Check if a field should be treated as a date"""
        date_indicators = ['date', 'established', 'start', 'end', 'createdat', 'updatedat']
        return any(indicator in field_name.lower() for indicator in date_indicators)
    
    def _normalize_date(self, value: Any) -> Optional[str]:
        """Normalize date values to YYYY-MM-DD format"""
        if not value:
            return None
        
        date_str = str(value).strip()
        
        # Handle different date formats
        if 'T' in date_str:  # ISO datetime format
            return date_str.split('T')[0]
        elif len(date_str) == 4 and date_str.isdigit():  # Year only
            return f"{date_str}-01-01"
        elif len(date_str) == 10 and date_str.count('-') == 2:  # YYYY-MM-DD
            return date_str
        else:
            print(f"⚠️  Skipping invalid date format: {date_str}")
            return None

    def extract_relationships(self, record: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """Extract NocoDB relationship data from a record"""
        relationships = {}
        
        for key, value in record.items():
            # Handle many-to-many relationships
            if key.startswith('_nc_m2m_') and isinstance(value, list):
                relationships[key] = value
            
            # Handle single object relationships (like author, recipient)
            elif isinstance(value, dict) and 'Id' in value:
                relationships[f"object_{key}"] = [value['Id']]
        
        return relationships
    
    def map_relationships_to_baserow(self, relationships: Dict[str, List], 
                                   table_name: str) -> Dict[str, List[int]]:
        """Convert NocoDB relationships to Baserow link field format"""
        baserow_relationships = {}
        
        # Define relationship mappings
        relationship_mappings = {
            'Infrastructure': {
                '_nc_m2m_infrastructure_locations': {
                    'field_name': 'linked_location',
                    'source_table': 'Location',
                    'id_field': 'location_id'
                },
                '_nc_m2m_entity_infrastructures': {
                    'field_name': 'linked_entities',
                    'source_table': 'Entity',
                    'id_field': 'entity_id'
                },
                # Additional infrastructure relationships
                '_nc_m2m_infrastructure_discursive_oils': {
                    'field_name': 'linked_discursive_oil',
                    'source_table': 'Discursive-oil',
                    'id_field': 'discursive_oil_id'
                },
                '_nc_m2m_ecosystem_conse_infrastructures': {
                    'field_name': 'linked_ecosystem',
                    'source_table': 'Ecosystem',
                    'id_field': 'ecosystem_id'  # Note: May need to check actual field name
                },
                '_nc_m2m_related_events_infrastructures': {
                    'field_name': 'linked_related_events',
                    'source_table': 'Related-events',
                    'id_field': 'related_events_id'
                }
            },
            
            'Transactions': {
                '_nc_m2m_transactions_entities': {
                    'field_name': 'linked_entities',
                    'source_table': 'Entity',
                    'id_field': 'entity_id'
                },
                '_nc_m2m_transactions_people': {
                    'field_name': 'linked_people', 
                    'source_table': 'People',
                    'id_field': 'people_id'
                },
                '_nc_m2m_transactions_primary_sources': {
                    'field_name': 'linked_sources',
                    'source_table': 'Source',
                    'id_field': 'primary_sources_id'
                },
                # Additional transaction relationships
                '_nc_m2m_transactions_discursive_oils': {
                    'field_name': 'linked_discursive_oil',
                    'source_table': 'Discursive-oil',
                    'id_field': 'discursive_oil_id'
                }
            },
            
            'Discursive-oil': {
                'object_author': {
                    'field_name': 'linked_author',
                    'source_table': 'People',
                    'direct_id': True
                },
                'object_recipient': {
                    'field_name': 'linked_recipient', 
                    'source_table': 'People',
                    'direct_id': True
                },
                '_nc_m2m_discursive_oil_primary_sources': {
                    'field_name': 'linked_sources',
                    'source_table': 'Source',
                    'id_field': 'primary_sources_id'
                }
            },
            
            # People relationships
            'People': {
                '_nc_m2m_people_roles': {
                    'field_name': 'linked_roles',
                    'source_table': 'Role', 
                    'id_field': 'role_id'
                },
                '_nc_m2m_related_events_people': {
                    'field_name': 'linked_related_events',
                    'source_table': 'Related-events',
                    'id_field': 'related_events_id'
                },
                '_nc_m2m_actions-timelin_people': {
                    'field_name': 'linked_actions_timeline',
                    'source_table': 'Actions-timeline',
                    'id_field': 'actions_timeline_id'
                },
                '_nc_m2m_transactions_people': {
                    'field_name': 'linked_transactions',
                    'source_table': 'Transactions',
                    'id_field': 'transactions_id'
                }
            },
            
            # Role relationships  
            'Role': {
                '_nc_m2m_people_roles': {
                    'field_name': 'linked_people',
                    'source_table': 'People',
                    'id_field': 'people_id'
                },
                '_nc_m2m_role_entities': {
                    'field_name': 'linked_entities',
                    'source_table': 'Entity',
                    'id_field': 'entity_id'
                },
                '_nc_m2m_role_locations': {
                    'field_name': 'linked_locations',
                    'source_table': 'Location',
                    'id_field': 'location_id'
                }
            },
            
            # Actions-timeline relationships
            'Actions-timeline': {
                '_nc_m2m_actions-timelin_people': {
                    'field_name': 'linked_people',
                    'source_table': 'People',
                    'id_field': 'people_id'
                }
            },
            
            # Related-events relationships
            'Related-events': {
                '_nc_m2m_related_events_people': {
                    'field_name': 'linked_people',
                    'source_table': 'People',
                    'id_field': 'people_id'
                },
                '_nc_m2m_related_events_infrastructures': {
                    'field_name': 'linked_infrastructures',
                    'source_table': 'Infrastructure',
                    'id_field': 'infrastructure_id'
                }
            },
            
            # Memory relationships (if relationship fields exist)
            'Memory': {
                # Note: These relationships would need corresponding fields created in Baserow
            }
        }
        
        # Get mappings for this table
        table_mappings = relationship_mappings.get(table_name, {})
        
        for rel_key, rel_data in relationships.items():
            if rel_key not in table_mappings or not rel_data:
                continue
            
            mapping = table_mappings[rel_key]
            field_name = mapping['field_name']
            source_table = mapping['source_table']
            
            # Find the corresponding field in the schema
            schema = self.table_schemas.get(table_name)
            if not schema:
                continue
                
            field_info = schema.get_field_by_name(field_name)
            if not field_info:
                continue
            
            field_id = f"field_{field_info.id}"
            
            # Convert old IDs to new Baserow IDs
            new_ids = []
            
            if mapping.get('direct_id'):
                # Direct ID mapping (for object relationships)
                for old_id in rel_data:
                    new_id = self.id_mappings.get(source_table, {}).get(old_id)
                    if new_id:
                        new_ids.append(new_id)
            else:
                # Relationship table mapping
                id_field = mapping['id_field']
                for rel in rel_data:
                    if isinstance(rel, dict):
                        old_id = rel.get(id_field)
                        if old_id:
                            new_id = self.id_mappings.get(source_table, {}).get(old_id)
                            if new_id:
                                new_ids.append(new_id)
            
            if new_ids:
                baserow_relationships[field_id] = new_ids
        
        return baserow_relationships
    
    def import_table_data(self, json_file_path: str, table_name: str, 
                         dry_run: bool = False, clear_table: bool = False) -> bool:
        """Import data from JSON file to Baserow table"""
        print(f"\n🔄 Processing {table_name}...")
        
        # Get table info
        table_id = self.table_mappings.get(table_name)
        if not table_id:
            print(f"❌ Table '{table_name}' not found in mappings")
            return False
        
        # Clear existing data if requested
        if clear_table and not dry_run:
            print(f"  🗑️  Clearing existing data...")
            deleted_count = self.client.clear_table(table_id)
            print(f"  🗑️  Deleted {deleted_count} existing records")
        
        # Load JSON data
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
        except Exception as e:
            print(f"❌ Error loading {json_file_path}: {e}")
            return False
        
        # Handle different JSON structures
        if isinstance(json_data, list):
            items = json_data
        elif isinstance(json_data, dict) and 'list' in json_data:
            items = json_data['list']
        else:
            print(f"❌ Unexpected JSON structure in {json_file_path}")
            return False
        
        print(f"📊 Found {len(items)} records to import")
        
        if dry_run:
            print("🔍 DRY RUN - No data will be modified")
            return True
        
        # Create field mapping
        field_mapping = self.create_field_mapping(table_name)
        if not field_mapping:
            print(f"⚠️  No field mapping for {table_name}, using automatic mapping")
        
        # Import statistics
        success_count = 0
        error_count = 0
        
        # Process each record
        for i, item in enumerate(items, 1):
            try:
                # Extract relationships first
                relationships_data = self.extract_relationships(item)
                
                # Transform the core data
                cleaned_data = self.transform_record_data(item, field_mapping, table_name)
                
                # Create the record
                if cleaned_data:  # Only create if we have data
                    result = self.client.create_row(table_id, cleaned_data)
                    
                    # Store ID mapping
                    old_id = item.get('Id')
                    if old_id and result:
                        if table_name not in self.id_mappings:
                            self.id_mappings[table_name] = {}
                        self.id_mappings[table_name][old_id] = result['id']
                    
                    # Handle relationships (in a second pass)
                    if relationships_data and result:
                        baserow_relationships = self.map_relationships_to_baserow(
                            relationships_data, table_name
                        )
                        if baserow_relationships:
                            self.client.update_row(table_id, result['id'], baserow_relationships)
                    
                    success_count += 1
                    print(f"  ✅ Record {i}/{len(items)} - Success (ID: {result.get('id')})")
                else:
                    error_count += 1
                    print(f"  ⚠️  Record {i}/{len(items)} - No valid data to import")
                
            except Exception as e:
                error_count += 1
                print(f"  ❌ Record {i}/{len(items)} - Error: {e}")
                import traceback
                traceback.print_exc()
            
            # Rate limiting
            time.sleep(0.1)
        
        # Store statistics
        self.migration_stats[table_name] = {
            'success': success_count,
            'errors': error_count,
            'total': len(items)
        }
        
        print(f"\n📈 Import Summary for {table_name}:")
        print(f"  ✅ Success: {success_count}")
        print(f"  ❌ Errors: {error_count}")
        print(f"  📊 Total: {len(items)}")
        
        return success_count > 0
    
    def run_migration(self, dry_run: bool = False, clear_tables: bool = False, 
                     target_table: Optional[str] = None):
        """Run the complete migration process"""
        print("🚀 Starting CamillaDataset Migration...")
        print(f"Base URL: {self.base_url}")
        print(f"Database ID: {self.database_id}")
        print(f"{'='*60}")
        
        # First discover the actual table IDs
        if not self.discover_tables():
            print("❌ Cannot proceed without required tables")
            return
        
        # Initialize schemas
        self.initialize_schemas()
        
        # Filter import order if specific table requested
        import_order = self.import_order
        if target_table:
            import_order = [(filename, table_name) for filename, table_name in import_order 
                          if table_name == target_table]
            if not import_order:
                print(f"❌ Table '{target_table}' not found in import order")
                return
        
        # Process files in dependency order
        json_dir = os.path.join("data", "JSON")
        success_count = 0
        
        for phase_num, (filename, table_name) in enumerate(import_order, 1):
            print(f"\n--- Phase {phase_num}/{len(import_order)}: {table_name} ---")
            
            json_file_path = os.path.join(json_dir, filename)
            if not os.path.exists(json_file_path):
                print(f"⚠️  File not found: {filename} - skipping")
                continue
            
            # Import the table
            if self.import_table_data(json_file_path, table_name, dry_run, clear_tables):
                success_count += 1
            
            # Pause between tables
            if phase_num < len(import_order):
                print("⏳ Pausing before next table...")
                time.sleep(2)
        
        # Final summary
        self.print_final_summary(success_count, len(import_order), dry_run)
    
    def print_final_summary(self, success_count: int, total_count: int, dry_run: bool):
        """Print comprehensive migration summary"""
        print(f"\n{'='*60}")
        print("🎯 MIGRATION SUMMARY")
        print(f"{'='*60}")
        
        if dry_run:
            print("🔍 DRY RUN COMPLETED - No data was modified")
        else:
            print(f"📁 Tables processed: {success_count}/{total_count}")
            print(f"📈 Success rate: {(success_count/total_count*100):.1f}%")
        
        # Detailed statistics
        print(f"\n📊 Detailed Results:")
        for table_name, stats in self.migration_stats.items():
            success = stats['success']
            errors = stats['errors'] 
            total = stats['total']
            rate = (success/total*100) if total > 0 else 0
            print(f"  {table_name:<20}: {success:>4}/{total:<4} ({rate:>5.1f}%)")
        
        # ID mappings summary
        print(f"\n🔗 ID Mappings Created:")
        for table_name, mappings in self.id_mappings.items():
            print(f"  {table_name:<20}: {len(mappings):>4} records")
        
        if success_count == total_count and not dry_run:
            print("\n🎉 Migration completed successfully!")
        elif success_count < total_count:
            print(f"\n⚠️  {total_count - success_count} tables had issues - check logs above")
        
        print(f"{'='*60}")


def main():
    """Main entry point with command line argument support"""
    parser = argparse.ArgumentParser(description="CamillaDataset Migration to Baserow")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Preview changes without modifying data")
    parser.add_argument("--clear", action="store_true",
                       help="Clear existing table data before import")
    parser.add_argument("--table", type=str,
                       help="Import specific table only")
    
    args = parser.parse_args()
    
    try:
        # Create migration manager
        migrator = CamillaMigrationManager()
        
        # Run migration
        migrator.run_migration(
            dry_run=args.dry_run,
            clear_tables=args.clear,
            target_table=args.table
        )
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())