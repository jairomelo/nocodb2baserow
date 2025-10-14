"""
Baserow API Client - Generic REST API wrapper with rate limiting
"""

import requests
from typing import Dict, List, Optional
import time


class BaserowClient:
    """Generic Baserow API client with rate limiting and error handling"""
    
    def __init__(self, base_url: str, token: str, rate_limit_delay: float = 0.1, jwt_token: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.jwt_token = jwt_token
        self.rate_limit_delay = rate_limit_delay
        
        # Session for data operations (uses API Token)
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Token {token}',
            'Content-Type': 'application/json'
        })
        
        # Session for structural operations (uses JWT Token)
        self.jwt_session = requests.Session()
        if jwt_token:
            self.jwt_session.headers.update({
                'Authorization': f'JWT {jwt_token}',
                'Content-Type': 'application/json'
            })
    
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
                print("✅ JWT token obtained successfully")
                return jwt_token
            
        except Exception as e:
            print(f"❌ Error getting JWT token: {e}")
            return None

    def _make_request(self, method: str, endpoint: str, use_jwt: bool = False, **kwargs) -> requests.Response:
        """Make rate-limited API request with error handling"""
        time.sleep(self.rate_limit_delay)
        
        url = f"{self.base_url}/api/{endpoint.lstrip('/')}"
        session = self.jwt_session if use_jwt else self.session
        
        response = session.request(method, url, **kwargs)
        
        if not response.ok:
            print(f"API Error {response.status_code}: {response.text}")
            response.raise_for_status()
        
        return response
    
    def get_table_fields(self, table_id: int) -> List[Dict]:
        """Get all fields for a table"""
        response = self._make_request('GET', f'/database/fields/table/{table_id}/')
        return response.json()
    
    def get_table_rows(self, table_id: int, page: int = 1, size: int = 200) -> Dict:
        """Get rows from a table with pagination"""
        params = {'page': page, 'size': size}
        response = self._make_request('GET', f'/database/rows/table/{table_id}/', params=params)
        return response.json()
    
    def create_row(self, table_id: int, data: Dict) -> Dict:
        """Create a new row in a table"""
        response = self._make_request('POST', f'/database/rows/table/{table_id}/', json=data)
        return response.json()
    
    def update_row(self, table_id: int, row_id: int, data: Dict) -> Dict:
        """Update an existing row"""
        response = self._make_request('PATCH', f'/database/rows/table/{table_id}/{row_id}/', json=data)
        return response.json()
    
    def delete_row(self, table_id: int, row_id: int) -> bool:
        """Delete a row"""
        try:
            self._make_request('DELETE', f'/database/rows/table/{table_id}/{row_id}/')
            return True
        except:
            return False
    
    def clear_table(self, table_id: int) -> int:
        """Clear all rows from a table"""
        deleted_count = 0
        page = 1
        
        while True:
            data = self.get_table_rows(table_id, page=page)
            rows = data.get('results', [])
            
            if not rows:
                break
            
            for row in rows:
                if self.delete_row(table_id, row['id']):
                    deleted_count += 1
        
        return deleted_count
    
    def get_database_tables(self, database_id: int) -> List[Dict]:
        """Get all tables in a database"""
        response = self._make_request('GET', f'/database/tables/database/{database_id}/', use_jwt=True)
        return response.json()