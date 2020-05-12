# analyze test data for relationship between logs and artist-songs

import os
import glob
import psycopg2
import json

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    return all_files    

def setup():
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return conn, cur
    
def main():

    log_table_create = ("""
    CREATE TABLE IF NOT EXISTS log_work(
        log_id SERIAL PRIMARY KEY,
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length numeric,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        sessionId varchar,
        song varchar,
        status varchar,
        ts bigint,
        userAgent varchar,
        userId varchar)
    """)

    log_table_drop = "DROP TABLE IF EXISTS log_work"

    log_fields = [
        'artist', #varchar
        'auth', #varchar
        'firstName', #varchar
        'gender', #varchar
        'itemInSession', #int
        'lastName', #varchar
        'length', #numeric
        'level', #varchar
        'location', #varchar
        'method', #varchar
        'page', #varchar
        'registration', #varchar
        'sessionId', #int
        'song', #varchar
        'status', #varchar
        'ts', #bigint
        'userAgent', #varchar
        'userId' #int
    ]

    log_insert = "INSERT INTO log_work VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_work(
        num_songs int,
        artist_id varchar,
        artist_latitude varchar,
        artist_longitude varchar,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration numeric,
        year int
    )
    """)

    song_table_drop = "DROP TABLE IF EXISTS song_work"

    song_fields = [
        'num_songs', #int,
        'artist_id', #varchar,
        'artist_latitude', #varchar,
        'artist_longitude', #varchar,
        'artist_location', #varchar,
        'artist_name', #varchar,
        'song_id', #varchar,
        'title', #varchar,
        'duration', #numeric,
        'year', #int
    ]

    song_insert = "INSERT INTO song_work VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    conn, cur = setup()

    cur.execute(log_table_drop)
    conn.commit()

    cur.execute(log_table_create)
    conn.commit()

    cur.execute(song_table_drop)
    conn.commit()

    cur.execute(song_table_create)
    conn.commit()  
    
    log_files = get_files("data/log_data")
    #print(log_files)

    # load log files
    for filepath in log_files:
        log_data = []
        with open(filepath) as f:
            for line in f:
                log_data.append(json.loads(line))

        for item in log_data:
            if item['page'] != 'NextSong': continue
            log_rec = [item[field] for field in log_fields]
            #print(log_rec)
            cur.execute(log_insert, log_rec)

        conn.commit()

    song_files = get_files("data/song_data")
    #print(song_files)

    # load song files
    for filepath in song_files:
        song_data = []
        with open(filepath) as f:
            for line in f:
                song_data.append(json.loads(line))

        for item in song_data:
            song_rec = [item[field] for field in song_fields]
            #print(song_rec)
            cur.execute(song_insert, song_rec)

        conn.commit()

    cur.execute("SELECT COUNT(*) FROM song_work")
    count = cur.fetchone()
    print('Number of song records:', count[0])

    cur.execute("SELECT COUNT(*) FROM log_work")
    count = cur.fetchone()
    print('Number of log records:', count[0])
    
    cur.execute("SELECT COUNT(DISTINCT(song || artist || TO_CHAR(length, '99999D99999'))) FROM log_work")
    count = cur.fetchone()
    print('Number of distinct songs in log records:', count[0])
    
    match_query = """
    SELECT COUNT(*)
      FROM
      (SELECT artist, song, length
         FROM log_work where (artist, song, length) IN (SELECT artist_name, title, duration
                                                          FROM song_work)) AS foo;
    """
    cur.execute(match_query)
    count = cur.fetchone()
    print('Number of song records matched from log records:', count[0])

    match_query = """
    SELECT *
      FROM log_work WHERE (artist, song, length) IN (SELECT artist_name, title, duration
                                                       FROM song_work);
    """
    cur.execute(match_query)
    result = cur.fetchone()

    print('Matched row:', result)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

