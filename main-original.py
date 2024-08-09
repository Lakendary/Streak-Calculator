import pandas as pd
from datetime import datetime, timedelta

# Load the CSV files
habits_df = pd.read_csv('habits.csv')
daily_habit_tracker_df = pd.read_csv('daily_habit_tracker.csv')
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