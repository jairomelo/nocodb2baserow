"""
Data Transformation - Clean and validate data for Baserow
"""

from datetime import datetime, date
from typing import Any, Dict, Tuple, Optional
import re
from .schema_analyzer import TableSchema, FieldInfo


class DataTransformer:
    """Transform and validate data for Baserow"""
    
    def __init__(self):
        self.date_patterns = [
            (re.compile(r'^\d{4}$'), lambda m: f"{m.group()}-01-01"),  # Year: "1961"
            (re.compile(r'^\d{4}-\d{2}-\d{2}T'), lambda m: m.group()[:10]),  # ISO: "2025-04-18T..."
            (re.compile(r'^\d{4}-\d{2}-\d{2}$'), lambda m: m.group()),  # Already formatted
        ]
    
    def transform_record(self, record: Dict[str, Any], field_mapping: Dict[str, str], 
                        schema: TableSchema) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Transform NocoDB record to Baserow format"""
        cleaned_data = {}
        relationships = {}
        errors = []
        
        for json_field, value in record.items():
            # Handle relationships
            if json_field.startswith('_nc_m2m_'):
                relationships[json_field] = value
                continue
            
            # Skip unmapped fields
            if json_field not in field_mapping:
                continue
            
            # Get Baserow field info
            baserow_field_id = field_mapping[json_field]
            field_info = self._get_field_info_by_id(schema, baserow_field_id)
            
            # Transform value based on field type
            try:
                cleaned_value = self._transform_value(value, field_info)
                if cleaned_value is not None:
                    cleaned_data[baserow_field_id] = cleaned_value
            except Exception as e:
                errors.append(f"Error transforming {json_field}: {e}")
        
        if errors:
            print(f"⚠️  Transform errors: {'; '.join(errors)}")
        
        return cleaned_data, relationships
    
    def _get_field_info_by_id(self, schema: TableSchema, field_id: str) -> Optional[FieldInfo]:
        """Get field info by field_id string"""
        field_number = int(field_id.replace('field_', ''))
        for field in schema.fields:
            if field.id == field_number:
                return field
        return None
    
    def _transform_value(self, value: Any, field_info: Optional[FieldInfo]) -> Any:
        """Transform value based on field type"""
        if value is None or value == "":
            return None
        
        if not field_info:
            return str(value)
        
        # Type-specific transformations
        if field_info.type == 'date':
            return self._normalize_date(value)
        elif field_info.type == 'boolean':
            return self._normalize_boolean(value)
        elif field_info.type == 'number':
            return self._normalize_number(value)
        else:
            return str(value)
    
    def _normalize_date(self, value: Any) -> Optional[str]:
        """Normalize date values to YYYY-MM-DD format"""
        if not value:
            return None
        
        date_str = str(value).strip()
        
        for pattern, formatter in self.date_patterns:
            match = pattern.match(date_str)
            if match:
                return formatter(match)
        
        # If no pattern matches, return None to skip invalid dates
        print(f"⚠️  Invalid date format: {value}")
        return None
    
    def _normalize_boolean(self, value: Any) -> bool:
        """Normalize boolean values"""
        if isinstance(value, bool):
            return value
        
        str_value = str(value).lower()
        return str_value in ['true', '1', 'yes', 'on']
    
    def _normalize_number(self, value: Any) -> Optional[float]:
        """Normalize numeric values"""
        try:
            return float(value) if value != "" else None
        except (ValueError, TypeError):
            return None