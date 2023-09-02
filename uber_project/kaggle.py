import pandas as pd

df = pd.read_csv('Driver_App_Analytics.csv')
print(df.head(10))

df['Event Time (UTC)'] = pd.to_datetime(df['Event Time (UTC)'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
print(df.info())

fact = df.drop_duplicates().reset_index(drop=True)
print(fact.head)
pick_up = df[['Event Time (UTC)']].reset_index(drop=True)
pick_up['pick_up_Year'] = pick_up['Event Time (UTC)'].dt.year
pick_up['pick_up_hour'] = pick_up['Event Time (UTC)'].dt.hour
pick_up['pick_up_day'] = pick_up['Event Time (UTC)'].dt.day
pick_up['pick_up_id']=pick_up.index
pick_up=pick_up[['pick_up_id','pick_up_Year','pick_up_hour','pick_up_day']]
print(pick_up.head())