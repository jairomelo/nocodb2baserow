"""
Baserow Table Structure Creator
Creates all necessary tables with proper schema and dependencies for the Camilla Dataset
Based on Baserow API: https://api.baserow.io/api/redoc/
"""

import requests
import time
import os
from typing import List, Dict, Optional
import dotenv

# Load environment variables
dotenv.load_dotenv()

# Configuration
BASEROW_BASE_URL =  os.getenv("BASEROW_BASE_URL")  
DATABASE_ID = os.getenv("DATABASE_ID")  
API_TOKEN = os.getenv("API_TOKEN")  
JWT_TOKEN = os.getenv("JWT_TOKEN") 
USER_EMAIL = os.getenv("USER_EMAIL")
USER_PASSWORD = os.getenv("USER_PASSWORD")

class BaserowTableCreator:
    """Creates tables and fields in Baserow database"""
    
    def __init__(self, base_url: str, database_id: int, jwt_token: Optional[str] = None, api_token: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.database_id = database_id
        self.jwt_token = jwt_token
        self.api_token = api_token
        
        # Session for table/field creation (requires JWT)
        self.jwt_session = requests.Session()
        if jwt_token:
            self.jwt_session.headers.update({
                'Authorization': f'JWT {jwt_token}',
                'Content-Type': 'application/json'
            })
        
        # Session for data operations (uses Database Token)
        self.api_session = requests.Session()
        if api_token:
            self.api_session.headers.update({
                'Authorization': f'Token {api_token}',
                'Content-Type': 'application/json'
            })
        
        self.created_tables = {}  # Store table_name -> table_id mapping
    
    def create_table(self, name: str, init_with_data: bool = False) -> Optional[Dict]:
        """Create a new table in the database"""
        try:
            url = f"{self.base_url}/api/database/tables/database/{self.database_id}/"
            data = {
                "name": name,
                "init_with_data": False  # Always false to prevent default data/columns
            }
            
            response = self.jwt_session.post(url, json=data)
            response.raise_for_status()
            
            table_info = response.json()
            table_id = table_info.get('id')
            
            print(f"✅ Created table: {name} (ID: {table_id})")
            self.created_tables[name] = table_id
            return table_info
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error creating table {name}: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"Response: {e.response.text}")
            return None
    
    def create_field(self, table_id: int, field_config: Dict) -> Optional[Dict]:
        """Create a field in a table"""
        try:
            url = f"{self.base_url}/api/database/fields/table/{table_id}/"
            
            response = self.jwt_session.post(url, json=field_config)
            response.raise_for_status()
            
            field_info = response.json()
            print(f"  ➕ Added field: {field_config['name']} ({field_config['type']})")
            return field_info
            
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Error creating field {field_config['name']}: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"  Response: {e.response.text}")
            return None
    
    def get_field_types(self) -> List[Dict]:
        """Get available field types"""
        try:
            url = f"{self.base_url}/api/database/fields/types/"
            response = self.jwt_session.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching field types: {e}")
            return []
    
    def get_jwt_token(self, email: str, password: str) -> Optional[str]:
        """Get JWT token using email/password authentication"""
        try:
            url = f"{self.base_url}/api/user/token-auth/"
            data = {"email": email, "password": password}
            response = requests.post(url, json=data)
            response.raise_for_status()
            
            token_data = response.json()
            jwt_token = token_data.get('access_token')
            
            if jwt_token:
                self.jwt_token = jwt_token
                self.jwt_session.headers.update({
                    'Authorization': f'JWT {jwt_token}',
                    'Content-Type': 'application/json'
                })
                print("✅ JWT token refreshed successfully")
                return jwt_token
            
        except Exception as e:
            print(f"❌ Error getting JWT token: {e}")
            return None

def add_relationship_fields(creator: BaserowTableCreator, schemas: Dict[str, Dict]):
    """Add relationship fields (link to table) after all tables are created"""
    
    print(f"\n{'='*60}")
    print("ADDING KEY RELATIONSHIP FIELDS")
    print(f"{'='*60}")
    
    # Essential relationships for all tables
    relationships = {
        "Infrastructure": [
            {"name": "linked_location", "type": "link_row", "link_row_table_id": "Location"},
            {"name": "linked_entities", "type": "link_row", "link_row_table_id": "Entity"},
        ],
        
        "Transactions": [
            {"name": "linked_entities", "type": "link_row", "link_row_table_id": "Entity"},
            {"name": "linked_people", "type": "link_row", "link_row_table_id": "People"},
            {"name": "linked_sources", "type": "link_row", "link_row_table_id": "Source"},
        ],
        
        "Discursive_Oil": [
            {"name": "linked_author", "type": "link_row", "link_row_table_id": "People"},
            {"name": "linked_recipient", "type": "link_row", "link_row_table_id": "People"},
            {"name": "linked_sources", "type": "link_row", "link_row_table_id": "Source"},
        ],
        
        # NEW: People relationship fields
        "People": [
            {"name": "linked_roles", "type": "link_row", "link_row_table_id": "Role"},
            {"name": "linked_related_events", "type": "link_row", "link_row_table_id": "Related_Events"},
            {"name": "linked_actions_timeline", "type": "link_row", "link_row_table_id": "Actions_Timeline"},
            {"name": "linked_transactions", "type": "link_row", "link_row_table_id": "Transactions"},
        ],
        
        # NEW: Role relationship fields  
        "Role": [
            {"name": "linked_people", "type": "link_row", "link_row_table_id": "People"},
            {"name": "linked_entities", "type": "link_row", "link_row_table_id": "Entity"},
            {"name": "linked_locations", "type": "link_row", "link_row_table_id": "Location"},
        ],
        
        # NEW: Related-events relationship fields
        "Related_Events": [
            {"name": "linked_people", "type": "link_row", "link_row_table_id": "People"},
            {"name": "linked_infrastructures", "type": "link_row", "link_row_table_id": "Infrastructure"},
        ],
        
        # NEW: Actions-timeline relationship fields
        "Actions_Timeline": [
            {"name": "linked_people", "type": "link_row", "link_row_table_id": "People"},
        ],
    }
    
    # Add relationship fields to each table
    for table_name, field_list in relationships.items():
        if table_name not in creator.created_tables:
            print(f"⚠️  Table {table_name} not found, skipping relationships")
            continue
        
        table_id = creator.created_tables[table_name]
        print(f"\n📎 Adding relationships to {table_name}:")
        
        for field_config in field_list:
            # Replace table name with actual table ID
            if "link_row_table_id" in field_config:
                target_table = field_config["link_row_table_id"]
                if target_table in creator.created_tables:
                    field_config["link_row_table_id"] = creator.created_tables[target_table]
                    creator.create_field(table_id, field_config)
                else:
                    print(f"  ⚠️  Target table {target_table} not found for field {field_config['name']}")
    
    print(f"\n💡 Note: Only essential relationships added. More can be added later via Baserow UI.")

def main():
    """Create all tables with proper structure"""
    
    print(f"🚀 Starting Baserow table creation for database {DATABASE_ID}")
    print(f"Base URL: {BASEROW_BASE_URL}")
    print(f"{'='*60}")
    
    # Initialize creator without JWT token first
    creator = BaserowTableCreator(BASEROW_BASE_URL, DATABASE_ID, None, API_TOKEN)
    
    # Always get a fresh JWT token using email/password
    if USER_EMAIL and USER_PASSWORD:
        print("🔐 Getting fresh JWT token for table creation...")
        jwt_token = creator.get_jwt_token(USER_EMAIL, USER_PASSWORD)
        if not jwt_token:
            print("❌ Failed to get JWT token. Cannot create tables.")
            return
    else:
        print("❌ Error: USER_EMAIL and USER_PASSWORD required for table creation")
        print("Please set these values in your .env file")
        return
    
    print("✅ Authentication ready for table creation")
    
    # Get table schemas
    schemas_path = os.path.join("data", "JSON", "schemas.json")
    try:
        with open(schemas_path, 'r') as f:
            import json
            schemas = json.load(f)
        print(f"✅ Loaded table schemas from {schemas_path}")
    except Exception as e:
        print(f"❌ Error loading schemas: {e}")
        return
    
    # Import order (same as our data import order)
    creation_order = [
        # Phase 1: Foundation tables
        "Location", "Role", "Source",
        
        # Phase 2: Core entities  
        "People", "Entity",
        
        # Phase 3: Infrastructure
        "Infrastructure", "Licenses", "Ecosystem",
        
        # Phase 4: Transactions
        "Transactions", "Actions_Timeline", 
        
        # Phase 5: Communications
        "Discursive_Oil", "Related_Events", "Memory"
    ]
    
    print("\n📋 CREATING TABLES WITH BASIC FIELDS")
    print(f"{'='*60}")
    
    # Create tables in dependency order
    for i, table_name in enumerate(creation_order, 1):
        print(f"\n🔨 [{i}/{len(creation_order)}] Creating {table_name}...")
        
        # Create table
        table_info = creator.create_table(table_name)
        
        if not table_info:
            print(f"❌ Failed to create {table_name}, stopping.")
            return
        
        table_id = table_info['id']
        
        # Add fields to table
        schema = schemas.get(table_name, {})
        fields = schema.get('fields', [])
        
        print(f"  Adding {len(fields)} fields:")
        for field_config in fields:
            creator.create_field(table_id, field_config)
        
        # Small delay between tables
        time.sleep(1)
    
    # Add relationship fields
    add_relationship_fields(creator, schemas)
    
    # Summary
    print(f"\n{'='*60}")
    print("🎉 TABLE CREATION COMPLETE!")
    print(f"{'='*60}")
    print(f"Created {len(creator.created_tables)} tables:")
    
    for table_name, table_id in creator.created_tables.items():
        print(f"  ✅ {table_name}: {table_id}")
    
    # Generate updated TABLE_MAPPINGS for putData.py
    print(f"\n📝 TABLE_MAPPINGS for putData.py:")
    print("TABLE_MAPPINGS = {")
    for table_name, table_id in creator.created_tables.items():
        # Convert table names to match JSON file patterns
        mapping_name = table_name.replace('_', '-')
        if mapping_name == "Actions-Timeline":
            mapping_name = "Actions-timeline"
        elif mapping_name == "Discursive-Oil":
            mapping_name = "Discursive-oil"
        elif mapping_name == "Related-Events":
            mapping_name = "Related-events"
        
        print(f'    "{mapping_name}": {table_id},')
    print("}")

if __name__ == "__main__":
    main()