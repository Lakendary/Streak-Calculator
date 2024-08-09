import pandas as pd
import requests
from datetime import datetime

# Your token here
token = 'this is my secret'

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

    def notion_search(self, integration_token=token):
        url = "https://api.notion.com/v1/search"
        results = []
        next_cursor = None

        while True:
            if next_cursor:
                payload_dname["start_cursor"] = next_cursor

            response = requests.post(url, json=payload_dname, headers=headers)

            if response.status_code != 200:
                print(f'Error: {response.status_code}')
                return None
            else:
                response_json = response.json()
                results.extend(response_json["results"])
                next_cursor = response_json.get("next_cursor")

            if not next_cursor:
                break

        return {"results": results}

    def notion_db_details(self, database_id, integration_token=token):
        url = f"https://api.notion.com/v1/databases/{database_id}/query"
        response = requests.post(url, headers=headers)

        if response.status_code != 200:
            print(f'Error: {response.status_code}, {response.text}')
            return None
        else:
            return response.json()

    def get_table_data(self, data_json):
        if not data_json or "results" not in data_json:
            print(f'Invalid data_json: {data_json}')
            return {}

        columns = data_json["results"][0]["properties"].keys()
        table_data = {col: [] for col in columns}

        for row in data_json["results"]:
            for col in columns:
                cell = row["properties"][col]
                if cell["type"] == "title":
                    table_data[col].append(cell["title"][0]["plain_text"] if cell["title"] else "")
                elif cell["type"] == "rich_text":
                    table_data[col].append(cell["rich_text"][0]["plain_text"] if cell["rich_text"] else "")
                elif cell["type"] == "checkbox":
                    table_data[col].append(cell["checkbox"])
                elif cell["type"] == "number":
                    table_data[col].append(cell["number"])
                elif cell["type"] == "date":
                    table_data[col].append(cell["date"]["start"] if cell["date"] else "")
                elif cell["type"] == "select":
                    table_data[col].append(cell["select"]["name"] if cell["select"] else "")
                elif cell["type"] == "multi_select":
                    table_data[col].append(", ".join([opt["name"] for opt in cell["multi_select"]]))
                elif cell["type"] == "people":
                    table_data[col].append(", ".join([person["name"] for person in cell["people"]]))
                elif cell["type"] == "email":
                    table_data[col].append(cell["email"])
                elif cell["type"] == "phone_number":
                    table_data[col].append(cell["phone_number"])
                elif cell["type"] == "url":
                    table_data[col].append(cell["url"])
                else:
                    table_data[col].append(None)

        return table_data

if __name__ == '__main__':
    nsync = NotionSync()

    # Get habits data from Notion
    habit_data = nsync.notion_db_details('habit_id_goes_here')
    if habit_data:
        print("Habit data:", habit_data)
        habit_table = nsync.get_table_data(habit_data)
        habits_df = pd.DataFrame.from_dict(habit_table)
    else:
        print("Failed to retrieve habit data.")
        habits_df = pd.DataFrame()

    # Get daily habit tracker data from Notion
    daily_habit_data = nsync.notion_db_details('daily_habit_id_goes_here')
    if daily_habit_data:
        print("Daily habit tracker data:", daily_habit_data)
        daily_habit_table = nsync.get_table_data(daily_habit_data)
        daily_habit_tracker_df = pd.DataFrame.from_dict(daily_habit_table)
    else:
        print("Failed to retrieve daily habit tracker data.")
        daily_habit_tracker_df = pd.DataFrame()

    # Load the calendar CSV file
    calendar_df = pd.read_csv('calendar.csv')

    # Convert date columns to datetime
    daily_habit_tracker_df['date'] = pd.to_datetime(daily_habit_tracker_df['date'])
    calendar_df['date'] = pd.to_datetime(calendar_df['date'])

    # Sort the daily habit tracker by date
    daily_habit_tracker_df = daily_habit_tracker_df.sort_values(by='date')

    # Initialize the streaks dataframe
    streaks_df = pd.DataFrame(columns=['id', 'name', 'start_date', 'end_date', 'streak_count', 'extra', 'active'])
    streak_id = 1

    # Helper function to get the day of the week
    def get_day_of_week(date):
        return calendar_df.loc[calendar_df['date'] == date, 'day_of_week'].values[0]

    # Helper function to get the week number
    def get_week_number(date):
        return calendar_df.loc[calendar_df['date'] == date, 'week_number'].values[0]

    # Process each date in the daily habit tracker
    for index, row in daily_habit_tracker_df.iterrows():
        current_date = row['date']
        for habit in habits_df['name']:
            habit_freq = habits_df.loc[habits_df['name'] == habit, 'frequency'].values[0]
            habit_done = row[habit]
            
            # Check for active streak
            active_streak = streaks_df[(streaks_df['name'] == habit) & (streaks_df['active'] == True)]
            
            if habit_done:  # Habit completed
                if not active_streak.empty:
                    active_streak_index = active_streak.index[0]
                    if habit_freq == 'daily':
                        streaks_df.at[active_streak_index, 'streak_count'] += 1
                        streaks_df.at[active_streak_index, 'end_date'] = current_date
                    elif habit_freq == 'weekdays':
                        if get_day_of_week(current_date) in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                            streaks_df.at[active_streak_index, 'streak_count'] += 1
                            streaks_df.at[active_streak_index, 'end_date'] = current_date
                        else:
                            streaks_df.at[active_streak_index, 'extra'] += 1
                    elif habit_freq == 'weekly':
                        streak_end_week = get_week_number(streaks_df.at[active_streak_index, 'end_date'])
                        current_week = get_week_number(current_date)
                        if current_week == streak_end_week:
                            streaks_df.at[active_streak_index, 'extra'] += 1
                        elif current_week == streak_end_week + 1:
                            streaks_df.at[active_streak_index, 'streak_count'] += 1
                            streaks_df.at[active_streak_index, 'end_date'] = current_date
                else:
                    new_streak = {
                        'id': streak_id,
                        'name': habit,
                        'start_date': current_date,
                        'end_date': current_date,
                        'streak_count': 1,
                        'extra': 0,
                        'active': True
                    }
                    streaks_df = streaks_df._append(new_streak, ignore_index=True)
                    streak_id += 1
            else:  # Habit not completed
                if not active_streak.empty:
                    active_streak_index = active_streak.index[0]
                    if habit_freq == 'daily':
                        streaks_df.at[active_streak_index, 'active'] = False
                    elif habit_freq == 'weekdays':
                        if get_day_of_week(current_date) in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                            streaks_df.at[active_streak_index, 'active'] = False
                    elif habit_freq == 'weekly':
                        streak_end_week = get_week_number(streaks_df.at[active_streak_index, 'end_date'])
                        current_week = get_week_number(current_date)
                        if current_week >= streak_end_week + 2:
                            streaks_df.at[active_streak_index, 'active'] = False

    # Save the streaks dataframe to a CSV file
    streaks_df.to_csv('streaks.csv', index=False)
