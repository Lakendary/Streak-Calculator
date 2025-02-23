import requests
import pandas as pd
from datetime import datetime, timedelta
from configparser import ConfigParser
import numpy as np

def get_config():
    config = ConfigParser()
    config.read('config.ini')
    return config

config = get_config()

token = config['secret']['token']

payload_dname = {
    "filter": {
        "value": "database",
        "property": "object"
    },
    "page_size": 100
}

headers = {
    "Authorization": "Bearer " + token,
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

class NotionSync:
    def __init__(self):
        pass

    # search database name
    def  notion_search(self,integration_token = token):
        url = "https://api.notion.com/v1/search"
        results = []
        next_cursor = None

        while True:
            if next_cursor:
                payload_dname["start_cursor"] = next_cursor

            response = requests.post(url, json=payload_dname, headers=headers)

            if response.status_code != 200:
                return 'Error: ' + str(response.status_code)
                exit(0)
            else:
                response_json = response.json()
                results.extend(response_json["results"])
                next_cursor = response_json.get("next_cursor")

            if not next_cursor:
                break

        return {"results": results}

    # query database details
    def notion_db_details(self, database_id, integration_token=token):
        url = f"https://api.notion.com/v1/databases/{database_id}/query"
        results = []
        next_cursor = None

        while True:
            if next_cursor:
                payload = {"start_cursor": next_cursor}
            else:
                payload = {}

            response = requests.post(url, json=payload, headers=headers)

            if response.status_code != 200:
                return 'Error: ' + str(response.status_code)
                exit(0)
            else:
                response_json = response.json()
                results.extend(response_json["results"])
                next_cursor = response_json.get("next_cursor")

            if not next_cursor:
                break

        return {"results": results}

    # to get databases id and name
    def get_databases(self,data_json):
        databaseinfo = {}
        databaseinfo["database_id"] = [data_json["results"][i]["id"].replace("-","")
                                                for i in range(len(data_json["results"])) ]

        databaseinfo["database_name"] = [data_json["results"][i]["title"][0]["plain_text"]
                                                  if data_json["results"][i]["title"]
                                                  else ""
                                                  for i in range(len(data_json["results"])) ]

        databaseinfo["url"] = [ data_json["results"][i]["url"]
                                         if data_json["results"][i]["url"]
                                         else ""
                                         for i in range(len(data_json["results"])) ]
        return databaseinfo

    # to get column title of the table
    def get_tablecol_titles(self,data_json):
        return list(data_json["results"][0]["properties"].keys())
    
    # to get column data type for processing by type due to data structure is different by column type
    def get_tablecol_type(self,data_json,columns_title):
        type_data = {}
        for t in columns_title:
            type_data[t] = data_json["results"][0]["properties"][t]["type"]
        return type_data

    # to get table data by column type
    def get_table_data(self,data_json,columns_type):
        table_data = {}
        for k, v in columns_type.items():
            # to check column type and process by type
            if v in ["checkbox","number","email","phone_number"]:
                table_data[k] = [ data_json["results"][i]["properties"][k][v]
                                    if data_json["results"][i]["properties"][k][v]
                                    else ""
                                    for i in range(len(data_json["results"]))]
            elif v == "date":
                table_data[k] = [ data_json["results"][i]["properties"][k]["date"]["start"]
                                    if data_json["results"][i]["properties"][k]["date"]
                                    else ""
                                    for i in range(len(data_json["results"])) ]
            elif v == "rich_text" or v == 'title':
                table_data[k] = [ data_json["results"][i]["properties"][k][v][0]["plain_text"]
                                    if data_json["results"][i]["properties"][k][v]
                                    else ""
                                    for i in range(len(data_json["results"])) ]
            elif v == "files":
                table_data[k + "_FileName"] = [ data_json["results"][i]["properties"][k][v][0]["name"]
                                                if data_json["results"][i]["properties"][k][v]
                                                else ""
                                                for i in range(len(data_json["results"])) ]
                table_data[k + "_FileUrl"] = [ data_json["results"][i]["properties"][k][v][0]["file"]["url"]
                                           if data_json["results"][i]["properties"][k][v]
                                                else ""
                                           for i in range(len(data_json["results"])) ]
            elif v == "select":
                table_data[k] = [data_json["results"][i]["properties"][k][v]["name"]
                                    if data_json["results"][i]["properties"][k][v]
                                    else ""
                                    for i in range(len(data_json["results"]))]
            elif v == "people":
                table_data[k + "_Name"] = [ [data_json["results"][i]["properties"][k][v][j]["name"]
                                                if data_json["results"][i]["properties"][k][v]
                                                # to check if key 'name' exists in the list
                                                and "name" in data_json["results"][i]["properties"][k][v][j].keys()
                                                else ""
                                                for j in range(len(data_json["results"][i]["properties"][k][v]))]
                                                for i in range(len(data_json["results"])) ]
            elif v == "multi_select":
                table_data[k] = [ [data_json["results"][i]["properties"][k][v][j]["name"]
                                  if data_json["results"][i]["properties"][k][v]
                                  else ""
                                  for j in range(len(data_json["results"][i]["properties"][k][v]))]
                                  for i in range(len(data_json["results"])) 
                                ]

        return table_data    

def fill_missing_habit_data(daily_habit_tracker_df, habits_df):
    # Iterate over each habit (column) in the habits_df
    for habit in habits_df['Short Name']:
        # Check if the habit exists in the daily_habit_tracker_df
        if habit in daily_habit_tracker_df.columns:
            # Retrieve the corresponding 'Check' value for the habit
            check_property = habits_df.loc[habits_df['Short Name'] == habit, 'Check'].values[0]
            
            # Replace NaN values and empty strings based on the 'Check' value
            if check_property == 'Check':
                daily_habit_tracker_df[habit] = daily_habit_tracker_df[habit].replace(['', None], np.nan).fillna(False)
            elif check_property == 'Value':
                daily_habit_tracker_df[habit] = daily_habit_tracker_df[habit].replace(['', None], np.nan).fillna(0)
        else:
            print(f"Warning: Habit '{habit}' not found in daily_habit_tracker_df. Skipping.")
    
    return daily_habit_tracker_df

if __name__=='__main__':
    nsync = NotionSync()

    # to search all databases.
    data = nsync.notion_search()

    # to get database id and name.
    dbid_name = nsync.get_databases(data)

    #convert dictionary to dataframe.
    df = pd.DataFrame.from_dict(dbid_name)

    # convert to bool and then drop record with empty databasae name.
    df = df[df['database_name'].astype(bool)]
    
    # required dataframes for streaks
    daily_habit_tracker_df = pd.DataFrame()
    habits_df = pd.DataFrame()
    
    # required dataframes for streaks when testing
    # daily_habit_tracker_df = pd.read_csv(config['resources']['daily_habit_tracker_file'])
    # habits_df = pd.read_csv(config['resources']['habits_file'])

    # to loop through database id and get the database details.
    for d in dbid_name["database_id"]:
        
        if d == config['tables']['daily_habit_tracker'] or d == config['tables']['habits']:
            # notion given another API to get the details of databases by database id. search API does not return databases details.
            dbdetails = nsync.notion_db_details(d)
            # get column title
            columns_title = nsync.get_tablecol_titles(dbdetails)

            # get column type
            columns_type = nsync.get_tablecol_type(dbdetails,columns_title)

            # get table data
            table_data = nsync.get_table_data(dbdetails,columns_type)
            
            if d == config['tables']['daily_habit_tracker']:
                daily_habit_tracker_df = pd.DataFrame.from_dict(table_data)
            elif d == config['tables']['habits']:
                habits_df = pd.DataFrame.from_dict(table_data)

    # Columns to drop
    columns_to_drop = ['CC Balance Value', 'Focus Time (Mins)', 'Improvements', 'Lunch Feedback', 'Name', 'Status', 'Trees Died']
    daily_habit_tracker_df = daily_habit_tracker_df.drop(columns=columns_to_drop)
    
    calendar_df = pd.read_csv(config['resources']['calendar_file'])

    # Convert date columns to datetime
    daily_habit_tracker_df['Date'] = pd.to_datetime(daily_habit_tracker_df['Date'])
    calendar_df['date'] = pd.to_datetime(calendar_df['date'])

    # Rename 'date' column in calendar_df to 'Date' for merging purposes
    calendar_df = calendar_df.rename(columns={'date': 'Date'})

    # Merge the two dataframes on 'Date', ensuring all dates from calendar_df are included
    merged_df = pd.merge(calendar_df, daily_habit_tracker_df, on='Date', how='left')

    # Drop calendar-specific columns from the merged dataframe if they are not needed
    columns_to_drop = [col for col in calendar_df.columns if col != 'Date']
    merged_df = merged_df.drop(columns=columns_to_drop)

    # Sort the merged dataframe by 'Date'
    merged_df = merged_df.sort_values(by='Date')

    # Replace daily_habit_tracker_df with the newly merged dataframe
    daily_habit_tracker_df = merged_df

    # Replace NaN values with False or zero depending on check property
    daily_habit_tracker_df = fill_missing_habit_data(daily_habit_tracker_df, habits_df)

    # Rename 'Date' column in calendar_df to 'date' after merge is done
    calendar_df = calendar_df.rename(columns={'Date': 'date'})

    # Filter daily habit tracker dataframe to only have rows from 2025 onwards
    print("Number of rows in the daily habit tracker data frame before filter: ", len(daily_habit_tracker_df))
    daily_habit_tracker_df = daily_habit_tracker_df[daily_habit_tracker_df['Date'] >= '2025-01-01']
    print("Number of rows in the daily habit tracker data frame after filter: ", len(daily_habit_tracker_df))
    
    # Initialize the streaks dataframe
    streaks_df = pd.DataFrame(columns=['id', 'name', 'start_date', 'end_date', 'streak_count', 'extra', 'active'])
    streak_id = 1

    # Helper function to get the day of the week
    def get_day_of_week(date):
        return calendar_df.loc[calendar_df['date'] == date, 'day_of_week_name'].values[0]

    # Helper function to get the week number
    def get_week_number(date):
        return calendar_df.loc[calendar_df['date'] == date, 'week_number'].values[0]

    # Helper function to get the number of days left in the week
    def days_left_in_week(calendar_df, current_week_num, current_date):
        # Filter the calendar_df to get the dates in the current week
        current_week_dates = calendar_df[calendar_df['week_number'] == current_week_num]
        
        # Filter out dates that are before the current_date
        remaining_dates = current_week_dates[current_week_dates['date'] > current_date]
        
        # Return the number of remaining dates
        return len(remaining_dates)
    
    # Process each date in the daily habit tracker
    for index, row in daily_habit_tracker_df.iterrows():
        current_date = row['Date']
        for habit in habits_df['Short Name']:
            habit_freq = habits_df.loc[habits_df['Short Name'] == habit, 'Frequency'].values[0]
            habit_done = row.get(habit, None)  # Use .get() to handle missing columns
            
            # Check for active streak
            active_streak = streaks_df[(streaks_df['name'] == habit) & (streaks_df['active'] == True)]
            
            if habit_done:  # Habit completed
                if not active_streak.empty: # Active streak exists
                    active_streak_index = active_streak.index[0]
                    if habit_freq == 'Daily':
                        streaks_df.at[active_streak_index, 'streak_count'] += 1
                        streaks_df.at[active_streak_index, 'end_date'] = current_date
                    elif habit_freq == 'Weekdays':
                        if get_day_of_week(current_date) in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                            streaks_df.at[active_streak_index, 'streak_count'] += 1
                            streaks_df.at[active_streak_index, 'end_date'] = current_date
                        else:
                            streaks_df.at[active_streak_index, 'extra'] += 1
                    elif habit_freq == 'Weekly':
                        streak_end_week = get_week_number(streaks_df.at[active_streak_index, 'end_date'])
                        current_week = get_week_number(current_date)
                        if current_week == streak_end_week:
                            streaks_df.at[active_streak_index, 'extra'] += 1
                        elif current_week == streak_end_week + 1:
                            streaks_df.at[active_streak_index, 'streak_count'] += 1
                            streaks_df.at[active_streak_index, 'end_date'] = current_date
                    elif habit_freq == '3x-a-Week':
                        current_week_num = get_week_number(current_date)
                        streak_end_week_num = get_week_number(streaks_df.at[active_streak_index, 'end_date'])
                        streak_start_week_num = get_week_number(streaks_df.at[active_streak_index, 'start_date'])
                        streak_count = streaks_df.at[active_streak_index, 'streak_count']
                        
                        target_count = ((current_week_num - streak_start_week_num) + 1 ) * 3
                        if streak_count < target_count:
                            streaks_df.at[active_streak_index, 'streak_count'] += 1
                            streaks_df.at[active_streak_index, 'end_date'] = current_date
                        elif streak_count == target_count:
                            streaks_df.at[active_streak_index, 'extra'] += 1
                        
                        
                else: # No active streak exists
                    streak_count = 1
                    extra_count = 0

                    if habit_freq == 'Weekdays':
                        if get_day_of_week(current_date) in ['Saturday', 'Sunday']:
                            streak_count = 0
                            extra_count = 1

                    new_streak = {
                        'id': streak_id,
                        'name': habit,
                        'start_date': current_date,
                        'end_date': current_date,
                        'streak_count': streak_count,
                        'extra': extra_count,
                        'active': True
                    }
                    new_streak_df = pd.DataFrame([new_streak])
                    streaks_df = pd.concat([streaks_df, new_streak_df], ignore_index=True)
                    streak_id += 1
            elif habit_done == False or pd.isna(habit_done):  # Habit not completed or is NaN/empty
                if not active_streak.empty: # Active streak exists
                    active_streak_index = active_streak.index[0]
                    if habit_freq == 'Daily':
                        streaks_df.at[active_streak_index, 'active'] = False
                    elif habit_freq == 'Weekdays':
                        if get_day_of_week(current_date) in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                            streaks_df.at[active_streak_index, 'active'] = False
                    elif habit_freq == 'Weekly':
                        streak_end_week = get_week_number(streaks_df.at[active_streak_index, 'end_date'])
                        current_week = get_week_number(current_date)
                        if current_week >= streak_end_week + 2:
                            streaks_df.at[active_streak_index, 'active'] = False
                    elif habit_freq == '3x-a-Week':
                        current_week_num = get_week_number(current_date)
                        streak_end_week_num = get_week_number(streaks_df.at[active_streak_index, 'end_date'])
                        streak_start_week_num = get_week_number(streaks_df.at[active_streak_index, 'start_date'])
                        streak_count = streaks_df.at[active_streak_index, 'streak_count']

                        target_count = ((current_week_num - streak_start_week_num) + 1 ) * 3
                        days_left_num = days_left_in_week(calendar_df, current_week_num, current_date)

                        if streak_count + days_left_num < target_count:
                            streaks_df.at[active_streak_index, 'active'] = False
                        
    # Save the streaks dataframe to a CSV file
    streaks_df.to_csv(config['resources']['streaks_file'], index=False)