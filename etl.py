# etl.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 01/10/2020
# PURPOSE: Script to implement simple ETL processes for DWH tables for Data Engineering Project 1a.
#
# Included functions:
#     process_song_file   - extract data for song and artist dimensions from source song json files
#     process_song_file2  - extract data for song and artist dimensions from source song json files - using json library instead of pandas
#     process log_file    - extract data for user and time dimensions as well as songplay fact from source log/event json files
#     process_data        - utility function to handle os file processing for loading data
#     quality check       - perform basic quality check by counting rows in DWH tables
#     main                - main function performs ETL load
#  

import os
import glob
import psycopg2
import pandas as pd
import json
from time import time  
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Extract data for song and artist dimensions from source song json files
    Parameters:
      cur - cursor
      filepath - filepath to source data files
    """ 
    
    # open song file and read single record
    df_song = pd.read_json(filepath, lines=True)    

    # extract and process artist subset from df_song
    artist_data = pd.DataFrame(df_song, columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']).values[0].tolist()
    # insert artist record
    cur.execute(artist_table_insert, artist_data)  
    
    # extract and process song subset from df_song
    song_data = pd.DataFrame(df_song, columns = ['song_id', 'title', 'artist_id', 'year', 'duration']).values[0].tolist()
    # insert song record
    cur.execute(song_table_insert, song_data)
    
    
def process_song_file2(cur, filepath):
    """
    Extract data for song and artist dimensions from source song json files - using json library instead of pandas
    Parameters:
      cur - cursor
      filepath - filepath to source data files
    """ 
    
    # open song file and read single record
    with open(filepath) as json_file:
        df_song = json.load(json_file)
        
    # extract and process artist subset from df_song
    artist_data = list({k: df_song[k] for k in ('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')}.values())
    # insert artist record
    cur.execute(artist_table_insert, artist_data)      
        
    # extract and process song subset from df_song
    song_data = list({k: df_song[k] for k in ('song_id', 'title', 'artist_id', 'year', 'duration')}.values())
    # insert song record
    cur.execute(song_table_insert, song_data)

    
def process_log_file(cur, filepath):
    """
    extract data for user and time dimensions as well as songplay fact from source log/event json files
    Parameters:
      cur - cursor
      filepath - filepath to source data files
    """ 
    
    # open log file    
    df_log = pd.read_json(filepath, lines=True)
    
    df_log = df_log.loc[df_log['page'] == 'NextSong']    # filter dataframe to only include rows with page == 'NextSong'
    
    df_log['timestamp'] = pd.to_datetime(df_log['ts'], unit = 'ms')   # augment dataframe with a datetime version of the timestamp
    
    # extract and process time subset from log data
    time_df = pd.DataFrame(df_log['timestamp'])   # just extract timestamp into a new dataframe
    time_df['hour'] = time_df['timestamp'].dt.hour
    time_df['day'] = time_df['timestamp'].dt.day
    time_df['week'] = time_df['timestamp'].dt.week
    time_df['month'] = time_df['timestamp'].dt.month
    time_df['year'] = time_df['timestamp'].dt.year
    time_df['weekday'] = time_df['timestamp'].dt.weekday     # Monday=0, Sunday=6.   
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # extract and process user subset from log data
    user_df = pd.DataFrame(df_log, columns = ['userId', 'firstName', 'lastName', 'gender', 'level'])

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # extract and process songplay subset from logs 
    for index, row in df_log.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
    
        if results:
            songid, artistid = results
            # would not include this in production code but this is helpful since the test data has so few matches!
            print(f"Found match: song_id='{results[0]}', artist_id='{results[1]}' " +
                  f"- using criteria: song='{row.song}', artist='{row.artist}', length='{row.length}'.")
        else:
            songid, artistid = None, None
        
        # insert songplay record
        songplay_data = (row.timestamp, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    utility function to handle os file processing for loading data
    Parameters:
      cur - cursor
      conn - database connection
      filepath - filepath to source data files
      func - function to invoke to process data (i.e. process_log_file or process_song_file)
    """ 
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            if '-checkpoint' in f: 
                continue                          # filter out garbage files located in test data directory
            all_files.append(os.path.abspath(f))           

    # get total number of files found
    num_files = len(all_files)
    #print('{} files found in {}'.format(num_files, filepath))
    print(f'{num_files} files found in {filepath}')
    
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

        
def quality_check(cur, conn):
    """
    Perform basic quality check by counting rows in DWH tables
    Parameters:
      cur - cursor
      conn - database connection
    """ 
    
    # perform rudimentary quality check by counting rows in tables
    for table, query in count_table_queries.items():
        try:
            cur.execute(query)
            result = cur.fetchone()
            print(f"Count table query succeeded for table '{table}'. Total records: {result[0]}.") 
        except psycopg2.Error as e:
            print(f"Count table query failed for table '{table}'.")
            print(e)
            continue    
        

def main():
    """ 
    Main function contains core logic for ETL.
    Parameters: none
    """  
    
    start_time = time()
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # process the dimensions that can be derived from the song json files
    process_data(cur, conn, filepath='data/song_data', func=process_song_file2)
    
    # process the fact and dimensions that can be derived from the log json files
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    # perform a rudimentary quality check by counting rows loaded into tables
    quality_check(cur, conn)
    
    conn.close()
    
    tot_time = time() - start_time # calculate difference between end time and start time
    print("** Total Elapsed Runtime:",
          f"{str(int((tot_time/3600)))}:{str(int((tot_time%3600)/60))}:{str(round((tot_time%3600)%60))}")  


if __name__ == "__main__":
    main()