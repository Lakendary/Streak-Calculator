import requests
import pandas as pd
import json
import configparser
import os

def fetch_notion_table_data():
    """
    Fetch all rows from a specific Notion table for Power BI PowerQuery
    Reads configuration from config.ini file and uses new Notion API version 2025-09-03
    """
    
    # Load configuration from config.ini
    config = configparser.ConfigParser()
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.ini')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    config.read(config_path)
    
    # Read configuration values
    NOTION_TOKEN = config['secret']['token']
    DATABASE_ID = config['tables']['daily_habit_tracker']  # Using the daily_habit_tracker as default
    
    # Optional: Allow specific data source ID from config, otherwise auto-discover
    DATA_SOURCE_ID = config.get('tables', 'daily_habit_tracker_data_source', fallback=None)
    
    # Headers for Notion API with new version
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2025-09-03",
        "Content-Type": "application/json"
    }
    
    def get_data_source_id(database_id):
        """Get the data source ID from a database - required for new API version"""
        if DATA_SOURCE_ID:
            return DATA_SOURCE_ID
        
        # Get database info to find data sources
        url = f"https://api.notion.com/v1/databases/{database_id}"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f'Error getting database info: {response.status_code} - {response.text}')
        
        db_info = response.json()
        data_sources = db_info.get('data_sources', [])
        
        if not data_sources:
            raise Exception(f'No data sources found for database {database_id}')
        
        # Return the first data source ID (or all if you want to handle multiple)
        return data_sources[0]['id']
    
    def query_data_source(data_source_id):
        """Query the Notion data source using new API structure"""
        url = f"https://api.notion.com/v1/data_sources/{data_source_id}/query"
        results = []
        next_cursor = None
        
        while True:
            payload = {}
            if next_cursor:
                payload["start_cursor"] = next_cursor
            
            response = requests.post(url, json=payload, headers=headers)
            
            if response.status_code != 200:
                raise Exception(f'Error fetching data: {response.status_code} - {response.text}')
            
            response_json = response.json()
            results.extend(response_json["results"])
            next_cursor = response_json.get("next_cursor")
            
            if not next_cursor:
                break
        
        return {"results": results}
    
    def get_column_titles(data_json):
        """Extract column titles from the first record"""
        if not data_json["results"]:
            return []
        return list(data_json["results"][0]["properties"].keys())
    
    def get_column_types(data_json, columns_title):
        """Extract column types for proper data processing"""
        if not data_json["results"]:
            return {}
        
        type_data = {}
        for title in columns_title:
            type_data[title] = data_json["results"][0]["properties"][title]["type"]
        return type_data
    
    def extract_table_data(data_json, columns_type):
        """Extract and format table data based on column types - improved for better type handling"""
        table_data = {}
        
        for column_name, column_type in columns_type.items():
            # Handle checkbox properties (boolean values)
            if column_type == "checkbox":
                table_data[column_name] = [
                    record["properties"][column_name]["checkbox"] 
                    if record["properties"][column_name]["checkbox"] is not None
                    else False
                    for record in data_json["results"]
                ]
            
            # Handle number properties
            elif column_type == "number":
                table_data[column_name] = [
                    record["properties"][column_name]["number"]
                    if record["properties"][column_name]["number"] is not None
                    else 0
                    for record in data_json["results"]
                ]
            
            # Handle email and phone number properties
            elif column_type in ["email", "phone_number"]:
                table_data[column_name] = [
                    record["properties"][column_name][column_type]
                    if record["properties"][column_name][column_type] is not None
                    else ""
                    for record in data_json["results"]
                ]
            
            # Handle date properties
            elif column_type == "date":
                table_data[column_name] = [
                    record["properties"][column_name]["date"]["start"]
                    if record["properties"][column_name]["date"]
                    else ""
                    for record in data_json["results"]
                ]
            
            # Handle rich text and title properties
            elif column_type in ["rich_text", "title"]:
                table_data[column_name] = [
                    record["properties"][column_name][column_type][0]["plain_text"]
                    if record["properties"][column_name][column_type] and len(record["properties"][column_name][column_type]) > 0
                    else ""
                    for record in data_json["results"]
                ]
            
            # Handle select properties
            elif column_type == "select":
                table_data[column_name] = [
                    record["properties"][column_name]["select"]["name"]
                    if record["properties"][column_name]["select"]
                    else ""
                    for record in data_json["results"]
                ]
            
            # Handle multi_select properties
            elif column_type == "multi_select":
                table_data[column_name] = [
                    ", ".join([option["name"] for option in record["properties"][column_name]["multi_select"]])
                    if record["properties"][column_name]["multi_select"]
                    else ""
                    for record in data_json["results"]
                ]
            
            # Handle files properties
            elif column_type == "files":
                table_data[f"{column_name}_FileName"] = [
                    record["properties"][column_name]["files"][0]["name"]
                    if record["properties"][column_name]["files"]
                    else ""
                    for record in data_json["results"]
                ]
                table_data[f"{column_name}_FileUrl"] = [
                    record["properties"][column_name]["files"][0]["file"]["url"]
                    if record["properties"][column_name]["files"] and "file" in record["properties"][column_name]["files"][0]
                    else record["properties"][column_name]["files"][0]["external"]["url"]
                    if record["properties"][column_name]["files"] and "external" in record["properties"][column_name]["files"][0]
                    else ""
                    for record in data_json["results"]
                ]
            
            # Handle people properties
            elif column_type == "people":
                table_data[f"{column_name}_Names"] = [
                    ", ".join([person["name"] for person in record["properties"][column_name]["people"] if "name" in person])
                    if record["properties"][column_name]["people"]
                    else ""
                    for record in data_json["results"]
                ]
            
            # Handle URL properties
            elif column_type == "url":
                table_data[column_name] = [
                    record["properties"][column_name]["url"]
                    if record["properties"][column_name]["url"]
                    else ""
                    for record in data_json["results"]
                ]
            
            # Handle relation properties
            elif column_type == "relation":
                table_data[f"{column_name}_Relations"] = [
                    ", ".join([rel["id"] for rel in record["properties"][column_name]["relation"]])
                    if record["properties"][column_name]["relation"]
                    else ""
                    for record in data_json["results"]
                ]
            
            # Handle formula properties
            elif column_type == "formula":
                table_data[column_name] = [
                    record["properties"][column_name]["formula"]["string"]
                    if record["properties"][column_name]["formula"] and record["properties"][column_name]["formula"]["type"] == "string"
                    else record["properties"][column_name]["formula"]["number"]
                    if record["properties"][column_name]["formula"] and record["properties"][column_name]["formula"]["type"] == "number"
                    else record["properties"][column_name]["formula"]["boolean"]
                    if record["properties"][column_name]["formula"] and record["properties"][column_name]["formula"]["type"] == "boolean"
                    else ""
                    for record in data_json["results"]
                ]
            
            else:
                # For any other column types, try to extract safely
                table_data[column_name] = [
                    str(record["properties"][column_name])
                    for record in data_json["results"]
                ]
        
        return table_data
    
    try:
        # Get data source ID from database
        print("Discovering data source ID...")
        data_source_id = get_data_source_id(DATABASE_ID)
        print(f"Using data source ID: {data_source_id}")
        
        # Fetch data from Notion using new API
        print("Fetching data from Notion...")
        data = query_data_source(data_source_id)
        
        if not data["results"]:
            print("No data found in the data source.")
            return pd.DataFrame()
        
        # Extract column information
        columns_title = get_column_titles(data)
        columns_type = get_column_types(data, columns_title)
        
        print(f"Found columns: {columns_title}")
        print(f"Column types: {columns_type}")
        
        # Extract and format table data
        table_data = extract_table_data(data, columns_type)
        
        # Convert to DataFrame
        df = pd.DataFrame.from_dict(table_data)
        
        print(f"Successfully fetched {len(df)} rows and {len(df.columns)} columns")
        return df
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return pd.DataFrame()

# Main execution
if __name__ == "__main__":
    # Fetch the data
    notion_data = fetch_notion_table_data()
    
    # Display basic info about the data
    if not notion_data.empty:
        print("\nDataFrame Info:")
        print(f"Shape: {notion_data.shape}")
        print(f"Columns: {list(notion_data.columns)}")
        print("\nFirst few rows:")
        print(notion_data.head())
        
        # Optionally save to CSV for testing
        # notion_data.to_csv("notion_data_export.csv", index=False)
        # print("\nData saved to notion_data_export.csv")
    else:
        print("No data retrieved.")

# For Power BI PowerQuery, you can call this function directly:
# df = fetch_notion_table_data()

# Additional function to handle multiple data sources if needed
def fetch_all_data_sources(database_id=None):
    """
    Fetch data from all data sources in a database
    Useful if you upgrade to multi-source databases
    """
    if not database_id:
        config = configparser.ConfigParser()
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, 'config.ini')
        config.read(config_path)
        database_id = config['tables']['daily_habit_tracker']
    
    # Get all data sources for the database
    NOTION_TOKEN = config['secret']['token']
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2025-09-03",
        "Content-Type": "application/json"
    }
    
    url = f"https://api.notion.com/v1/databases/{database_id}"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f'Error getting database info: {response.status_code} - {response.text}')
    
    db_info = response.json()
    data_sources = db_info.get('data_sources', [])
    
    all_dataframes = []
    for i, data_source in enumerate(data_sources):
        print(f"Fetching data from data source {i+1}/{len(data_sources)}: {data_source['name']}")
        
        # Create a temporary function to fetch this specific data source
        temp_config = configparser.ConfigParser()
        temp_config.read_dict({
            'secret': {'token': NOTION_TOKEN},
            'tables': {
                'daily_habit_tracker': database_id,
                'daily_habit_tracker_data_source': data_source['id']
            }
        })
        
        df = fetch_notion_table_data()
        if not df.empty:
            df['data_source_name'] = data_source['name']
            df['data_source_id'] = data_source['id']
            all_dataframes.append(df)
    
    if all_dataframes:
        return pd.concat(all_dataframes, ignore_index=True)
    else:
        return pd.DataFrame()
