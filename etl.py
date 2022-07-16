import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from create_conn import create_connection


def process_song_file(cur, filepath):
    """
    Function the read song data, construct  data frame, segregate artist and song data. Finally, insert data
    in respective tables artists and songs.
    """
    # print("Processing song data")
    # read json files

    df = pd.read_json(filepath, typ='series')

    # Insert artist record first to avoid primary and foreign key validation errors
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']])
    print(f"INSERTING artist_data : {artist_data}")
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as err:
        print(f"Failed to insert Data. Reason : {err}")

    # Insert song record
    # To select columns for song ID, title, artist ID, year, and duration
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']])
    print(f"INSERTING song_data : {song_data}")
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as err:
        print(f"Failed to insert Data. Reason : {err}")


def process_log_file(cur, filepath):
    """
    Function the read data/log_data, construct  data frame, segregate artist and song data. Finally, insert data
    in respective tables  time, users and songplay.
    """
    # print("Processing Log data")
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query("page == 'NextSong'")

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')

    # Extract time from timestamp
    start_time = t

    # Extract hour from timestamp
    hour = t.dt.hour

    # Extract day/weekday from timestamp
    day = t.dt.day

    # Extract week of year/week  from timestamp
    week = t.dt.isocalendar().week

    # Extract Month from timestamp
    month = t.dt.month

    # Extract year from timestamp
    year = t.dt.year

    # Extract day/weekday from timestamp
    weekday = t.dt.day

    # Insert time data records
    time_data = (start_time, hour, day, week, month, year, weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')

    # Convert combining column_labels and time_data into a dictionary
    time_dict = dict(zip(column_labels, time_data))
    # Create a dataframe
    time_df = pd.DataFrame(data=time_dict)

    try:
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
    except psycopg2.Error as err:
        print(f"Failed to insert time data. Reason : {err}")

    # load user table
    # Select columns for user ID, first name, last name, gender and level and set to user_df
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # Insert user records
    try:
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
    except psycopg2.Error as err:
        print(f"Failed to insert user data. Reason : {err}")

    # Insert songplay records
    for index, row in df.iterrows():
        # Get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # print(f" SQL : {cur.mogrify(cur.query, list)}")
        # Insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid,
                         row.sessionId, row.location, row.userAgent)
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as err:
            print(f"Failed to insert song play data. Reason : {err}")


def process_data(cur, conn, filepath, func):
    """
    This is common method used for getting songs and log file to process.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f"{i}/{num_files} files processed.")


def main():
    """
    Method to connect database,and call process_data method  and finally close the DB connection.
    """
    # Connect to database
    try:
        print("Connecting to DB sparkifydb")
        # Connect to sparkifydb database and return cursor context and connection
        cur, conn = create_connection("sparkifydb", "student", "student")
    except psycopg2.Error as err:
        print(f"Error: Could not make connection to the Postgres database. Reason : {err}")

    # process song data
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)

    # process log data
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
