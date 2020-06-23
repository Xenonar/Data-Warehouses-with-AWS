import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE= config['IAM_ROLE']['ARN']
LOG_DATA= config['S3']['LOG_DATA']
LOG_JSONPATH=config['S3']['LOG_JSONPATH']
SONG_DATA=config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS statge_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
auth varchar,
artist varchar,
firstName varchar,
gender varchar,
itemInSession int,
lastName varchar,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration varchar,
sessionId int,
song varchar,
status int,
ts bigint,
userAgent varchar,
userId int);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
artist_id varchar,
artist_latitude float,
artist_longitude float,
artist_location varchar,
artist_name varchar,
duration float,
num_song int,
song_id varchar,
title varchar,
year int);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(
    "songplay_id" INTEGER IDENTITY(0,1) PRIMARY KEY,
    "start_time" TIMESTAMP  NOT NULL,
    "user_id" INTEGER NOT NULL,
    "level" VARCHAR NOT NULL,
    "song_id" VARCHAR NOT NULL,
    "artist_id" VARCHAR NOT NULL,
    "session_id" INTEGER NOT NULL,
    "location" VARCHAR ,
    "user_agent" VARCHAR NOT NULL
);
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users(
user_id INTEGER PRIMARY KEY,
first_name VARCHAR NOT NULL,
last_name VARCHAR NOT NULL,
gender VARCHAR,
level VARCHAR NOT NULL);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs(
song_id VARCHAR PRIMARY KEY,
title VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
year INTEGER,
duration FLOAT NOT NULL);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
artist_id VARCHAR PRIMARY KEY,
name VARCHAR NOT NULL,
location VARCHAR,
latitude FLOAT,
longitude FLOAT);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
start_time TIMESTAMP PRIMARY KEY,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday INTEGER);
""")


staging_events_copy = ("""COPY staging_events
from {}
iam_role{}
json {};
""").format(LOG_DATA,IAM_ROLE,LOG_JSONPATH)


staging_songs_copy = ("""
COPY staging_songs
from {}
iam_role{}
json 'auto';
""").format(SONG_DATA,IAM_ROLE)

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT timestamp 'epoch' +  staging_events.ts/1000 * interval '1 second' AS start_time,
staging_events.userId,
staging_events.level,
staging_songs.song_id,
staging_songs.artist_id,
staging_events.sessionId,
staging_events.location,
staging_events.userAgent
FROM staging_events
JOIN staging_songs
ON (staging_events.artist=staging_songs.artist_name
AND staging_events.song=staging_songs.title)
WHERE staging_events.page='NextSong';
""")

user_table_insert = (""" INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId,firstName,lastName,gender,level
FROM staging_events
WHERE page='NextSong';

""")

song_table_insert = (""" INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,title,artist_id,year,duration
FROM staging_songs
""")

artist_table_insert = (""" INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = (""" INSERT INTO time(start_time, hour, day, week, month, year) 
SELECT DISTINCT start_time, 
EXTRACT (hour FROM start_time), 
EXTRACT (day FROM start_time), 
EXTRACT (week FROM start_time),
EXTRACT (month FROM start_time),
EXTRACT (year FROM start_time)
FROM songplay;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
