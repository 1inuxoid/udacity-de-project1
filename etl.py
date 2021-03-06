import os
import glob
import psycopg2
import psycopg2.extras
import pandas as pd
from sql_queries import *
import csv
from io import StringIO


def process_song_file(cur, filepath):
    """
    Processes data from song file and populates songs and artists tables
    :param cur: connection cursor
    :param filepath: path to JSON file to load
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    song_data = list(map(lambda i: None if pd.isnull(i) or i == 0 else i, song_data))
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.values[0].tolist()
    # get rid of empty strings in values and handle NaN values for latitude/longitude
    artist_data = list(map(lambda i: None if pd.isnull(i) or (type(i) == str and len(i) == 0) else i, artist_data))
    cur.execute(artist_table_insert, artist_data)


def get_song_and_artist(row, cur):
    """
    Looks up song_id and artist_id columns for songplay table using log data
    :param row: row from log data frame
    :param cur: connection cursor used for querying
    :return: tuple (song_id, artist_id) which can be None if not found
    """
    cur.execute(song_select, (row.song, row.artist, row.length))
    results = cur.fetchone()

    if results:
        song_id, artist_id = results
    else:
        song_id, artist_id = None, None

    return song_id, artist_id


def process_log_file(cur, filepath):
    """
    - Processes data from files and persists it into times, users and songplays tables
    - Uses prepared statements in batch execution mode to speedup table updates
    :param cur: connection cursor
    :param filepath: path to JSON file to load
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_df = pd.DataFrame({
        'start_time': t,
        'hour': t.dt.hour,
        'day': t.dt.day,
        'week': t.dt.week,
        'month': t.dt.month,
        'year': t.dt.year,
        'weekday': t.dt.weekday
    })

    psycopg2.extras.execute_batch(cur, time_table_insert, time_df.values)

    # load user table
    user_df = pd.DataFrame({
        'user_id': df['userId'],
        'first_name': df['firstName'],
        'last_name': df['lastName'],
        'gender': df['gender'],
        'level': df['level']
    })
    user_df = user_df.drop_duplicates(subset=['user_id', 'first_name', 'last_name'], keep=False)
    # insert user records
    psycopg2.extras.execute_batch(cur, user_table_insert, user_df.values)

    # create songplay data frame
    songplay_data = pd.DataFrame({
        'start_time': pd.to_datetime(df['ts'], unit='ms'),
        'user_id': df['userId'],
        'level': df['level']
    }).join(
        [df[['song', 'artist', 'length']]
             .apply(get_song_and_artist, args=[cur], axis=1, result_type='expand')
             .rename(columns={0: 'song_id', 1: 'artist_id'}),
         pd.DataFrame({
             'session_id': df['sessionId'],
             'location': df['location'],
             # get rid of double quotes in some of the values
             'user_agent': df['userAgent'].str.replace('"', '')
         })
         ]
    )
    # insert songplay records
    psycopg2.extras.execute_batch(cur, songplay_table_insert, songplay_data.values)


def process_data(cur, conn, filepath, func):
    """
    Processes all JSON files in the given folder using the func function
    :param cur: connection cursor
    :param conn: connection to database
    :param filepath: path to files that have to be processed
    :param func: function that will be applied to each file, see process_song_fie, process_log_file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
