import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """
    Reads songs log file row by row, inserts them into song and artist tables.
    Parameters:
    cur (psycopg2.cursor()):cursor of the sparkifydb database
    filepath (str): Filepath of the file to be analyzed
    """
    # open song file
    df = df = pd.read_json(filepath,lines = True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].iloc[0,[0,1,2,3,4]].values
    song_data = song_data.tolist()
    song_data[3]=song_data[3].astype(int).astype(object)
    song_data[4]=song_data[4].astype(float).astype(object)
    song_data
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].iloc[0,[0,1,2,3,4]].values
    artist_data = artist_data.tolist()
    artist_data[3]=artist_data[3].astype(float).astype(object)
    artist_data[4]=artist_data[4].astype(float).astype(object)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """
    Reads User Activity log file row by row, filters by NexSong, transforms them and inserts
    them into time, user and songplay tables.
    Parameters:
    cur (psycopg2.cursor()): cursor of the sparkifydb database
    filepath (str): Filepath of the file to be analyzed
    """
    # open log file
    df = pd.read_json(filepath,lines = True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit = 'ms')
    
    # insert time data records
    time_data = []
    for record in t:
        time_data.append([record, record.hour, record.day, record.week, record.month, record.year, record.day_name()])
        column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')  
        
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df [['userId','firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    Iterates all files in filepath, and processes logs.
    Parameters:
    cur (psycopg2.cursor()): Cursor of the sparkifydb database
    conn (psycopg2.connect()): Connection to the sparkifycdb database
    filepath (str): Filepath of the logs to be analyzed
    func (python function): Function to be used to process each log
   
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
    Used to extract,transform data from song and user activity logs (entire datasets) and load it into a Postgres database
    Use python etl.py to run the script
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()