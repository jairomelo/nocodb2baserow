"""
Schema Analysis Classes - Understand table structures
"""

from typing import Dict, List, Optional
from dataclasses import dataclass
from .baserow_client import BaserowClient


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
    
    @property
    def date_fields(self) -> List[FieldInfo]:
        """Get all date fields"""
        return [field for field in self.fields if field.type == 'date']
    
    @property
    def relationship_fields(self) -> List[FieldInfo]:
        """Get all relationship fields"""
        return [field for field in self.fields if field.type == 'link_row']
    
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
    
    def print_schema_summary(self, schema: TableSchema):
        """Print a human-readable schema summary"""
        print(f"\n📋 {schema.table_name} (ID: {schema.table_id}) - {len(schema.fields)} fields:")
        print("=" * 70)
        
        for field in schema.fields:
            primary_marker = " [PRIMARY]" if field.primary else ""
            link_info = f" → table {field.linked_table_id}" if field.linked_table_id else ""
            print(f"  {field.name:<25} | field_{field.id:<8} | {field.type:<15}{primary_marker}{link_info}")