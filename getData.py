import os
import requests
import json
import time
from datetime import datetime
import dotenv

dotenv.load_dotenv()

URL = os.getenv("NOCODB_BASE_URL")
TOKEN = os.getenv("NOCODB_TOKEN")

def fetch_all_data(tableID, delay_between_requests=1):
    headers = {
        "xc-token": TOKEN
    }
    all_records = []
    page = 1
    request_count = 0
    
    while True:
        params = {
            "page": page
        }
        
        # Add delay between requests to avoid rate limiting
        if request_count > 0:
            time.sleep(delay_between_requests)
            
        try:
            response = requests.get(f"{URL}tables/{tableID}/records", headers=headers, params=params)
            response.raise_for_status()
            request_count += 1
            
            data = response.json()
            records = data.get("list", [])
            all_records.extend(records)
            
            print(f"  Page {page}: fetched {len(records)} records (total: {len(all_records)})")
            
            # Use 'isLastPage' to determine end of pagination
            if data.get("pageInfo", {}).get("isLastPage", True):
                break
            page += 1
            
        except requests.exceptions.RequestException as e:
            print(f"  Error on page {page}: {e}")
            # Retry with exponential backoff
            retry_delay = min(delay_between_requests * (2 ** (page % 3)), 10)
            print(f"  Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            continue
            
    print(f"  Completed: {request_count} requests made, {len(all_records)} total records")
    return all_records

def get_all_tables(csv_file, delay_between_tables=5, header=False):
    start_time = datetime.now()
    total_tables = 0
    successful_tables = 0
    total_records = 0
    
    print(f"Starting data collection at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    with open(csv_file, "r") as f:
        # skip first line if it's a header
        if header:
            next(f)
        for line_num, line in enumerate(f, 2):
            parts = line.strip().split(",")
            if len(parts) >= 2:
                table_name, table_id = parts[0], parts[1]
                total_tables += 1
                
                print(f"\n[{line_num}] Processing table: {table_name} (ID: {table_id})")
                
                try:
                    records = fetch_all_data(table_id)
                    
                    # Save to file
                    filename = f"{table_name}_data.json"
                    with open(filename, "w") as out_file:
                        json.dump(records, out_file, indent=4)
                    
                    successful_tables += 1
                    total_records += len(records)
                    print(f"  Saved {len(records)} records to {filename}")
                    
                except Exception as e:
                    print(f"  Failed to process table {table_name}: {e}")
                    continue
                
                # Add delay between tables to be respectful to the API
                if delay_between_tables > 0:
                    print(f"  Waiting {delay_between_tables} seconds before next table...")
                    time.sleep(delay_between_tables)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n{'='*50}")
    print(f"Data collection completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    print(f"Tables processed: {successful_tables}/{total_tables}")
    print(f"Total records collected: {total_records}")
    print(f"{'='*50}")


if __name__ == "__main__":
    get_all_tables("tables.csv")