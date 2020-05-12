# sql_queries.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 01/10/2020
# PURPOSE: Script encapsulating all SQL statements for Data Engineering Project 1a.   

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop =     "DROP TABLE IF EXISTS users"
song_table_drop =     "DROP TABLE IF EXISTS songs"
artist_table_drop =   "DROP TABLE IF EXISTS artists"
time_table_drop =     "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id BIGSERIAL,         
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR(256),
    song_id VARCHAR(256),
    artist_id VARCHAR(256),
    session_id INT NOT NULL,
    location VARCHAR(256),
    user_agent VARCHAR(256),
    PRIMARY KEY (songplay_id),
    FOREIGN KEY (start_time) REFERENCES time(start_time),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (song_id) REFERENCES songs(song_id),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
)
""")
# Note: I could add non unique indexes to start_time, user_id, song_id, artist_id to support FK function
#       but would have to do performance tuning to see if that was useful for typical queries to justify the overhead.
#       For an experiment I did add one such index on the songs dimension table below.
# Note: The song_id and artist_id FK cols should both be NOT NULL but that would require that every songplays row have corresponding
#       dimension rows in artists and songs tables. Currently this would require that a dummy row for 'Unknown Artist' and 'Unnown Song'
#       be added to those two dimensions respectively. Alternatively every artist/song referenced in the log/event data MUST have a 
#       referenced artist/song properly pre-loaded into the dimension tables. This is not currently the case with the test data
#       provided by Udacity. Therefore I'm leaving these columns nullable. In the following AWS DWH project I do create the dummy rows.
# Note: The sequence generated PK has a side effect of allowing duplicate rows to be inserted into this table. A solution would
#       require that the definition of the natural key of the source data be determined. Then that could be used as the primary key
#       of the table instead of the sequence generated key. Alternatively the sequence could be maintained as PK and a unique index could
#       be applied to the secondary, natural key. I suspect that a natural key for this data is some concatenation of columns such as
#       session_id, song_id or start_time and perhaps user_id. However without being able to study the source system/data in detail I'm
#       not sure about the best definition of that key so I'm leaving this as designed since it is not raising alarms with the reviewers.

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INT PRIMARY KEY,
    first_name VARCHAR(256),
    last_name VARCHAR(256),
    gender VARCHAR(256),
    level VARCHAR(256))
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(256) PRIMARY KEY,
    title VARCHAR(256),
    artist_id VARCHAR(256),
    year INT,
    duration NUMERIC(12,2),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
);
CREATE INDEX songs_artists_fk_idx ON songs (artist_id);
""")
# could add non unique index to artist_id to support FK function...
# experiment with adding one index to support one FK just to see how it works in Postgres...

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(256) PRIMARY KEY,
    name VARCHAR(256),
    location VARCHAR(256),
    latitude NUMERIC(20,6),
    longitude NUMERIC(20,6))
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT)
""")
# Note: This table should probably have a grain of one second or perhaps one minute. Currently it may have a grain of microseconds.
#       A side effect of this is that there is almost one time dimension row for every songplays fact row.

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) 
  DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    gender = EXCLUDED.gender,
    level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) 
  DO NOTHING
--  DO UPDATE SET
--    title = EXCLUDED.title,
--    artist_id = EXCLUDED.artist_id,
--    year = EXCLUDED.year,
--    duration = EXCLUDED.duration
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude) -- null lat/long convered to NaN? Fix...
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) 
  DO NOTHING
--  DO UPDATE SET
--    name = EXCLUDED.name,
--    location = EXCLUDED.location,
--    latitude = EXCLUDED.latitude,
--    longitude = EXCLUDED.longitude
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) 
  DO NOTHING
""")

# COUNT TABLES

songplay_table_count = "SELECT COUNT(*) FROM songplays"
user_table_count =     "SELECT COUNT(*) FROM users"
song_table_count =     "SELECT COUNT(*) FROM songs"
artist_table_count =   "SELECT COUNT(*) FROM artists"
time_table_count =     "SELECT COUNT(*) FROM time"

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id
  FROM songs s
  JOIN artists a ON s.artist_id = a.artist_id 
 WHERE s.title = %s
   AND a.name = %s
   AND ROUND(s.duration) = ROUND(%s)   -- relax match criteria slightly to avoid ETL rounding errors
""")

# QUERY LISTS

drop_table_queries   = {'songplay': songplay_table_drop, 
                        'song': song_table_drop, 
                        'artist': artist_table_drop, 
                        'user': user_table_drop, 
                        'time': time_table_drop}

create_table_queries = {'time': time_table_create,
                        'user': user_table_create, 
                        'artist': artist_table_create,
                        'song': song_table_create, 
                        'songplay': songplay_table_create}

count_table_queries = {'songplay': songplay_table_count, 
                        'song': song_table_count, 
                        'artist': artist_table_count, 
                        'user': user_table_count, 
                        'time': time_table_count}
