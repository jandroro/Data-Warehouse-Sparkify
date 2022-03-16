"""Define sql statements for deleting and creating staging, fact and dimension tables."""

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN_ROLE = config.get("IAM_ROLE", "ARN")
LOG_DATA_PATH = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA_PATH = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist            VARCHAR,
        auth              VARCHAR,
        firstName         VARCHAR,
        gender            VARCHAR,
        itemInSession     INT,
        lastName          VARCHAR,
        length            NUMERIC,
        level             VARCHAR,
        location          VARCHAR,
        method            VARCHAR,
        page              VARCHAR   SORTKEY,
        registration      BIGINT,
        sessionId         BIGINT,
        song              VARCHAR   DISTKEY,
        status            INT,
        ts                BIGINT,
        userAgent         VARCHAR,
        userId            INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs         INT,
        artist_id         VARCHAR   DISTKEY,
        artist_latitude   VARCHAR,
        artist_longitude  VARCHAR,
        artist_location   VARCHAR,
        artist_name       VARCHAR,
        song_id           VARCHAR,
        title             VARCHAR,
        duration          DECIMAL,
        year              INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id       BIGINT    IDENTITY(0,1)   SORTKEY,
        start_time        TIMESTAMP,
        user_id           INT,
        level             VARCHAR,
        song_id           VARCHAR   DISTKEY,
        artist_id         VARCHAR,
        session_id        INT,
        location          VARCHAR,
        user_agent        VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id           INT      SORTKEY   DISTKEY,
        first_name        VARCHAR,
        last_name         VARCHAR,
        gender            VARCHAR,
        level             VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id           VARCHAR  SORTKEY,
        title             VARCHAR,
        artist_id         VARCHAR,
        year              INT,
        duration          DECIMAL
    ) diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id         VARCHAR   SORTKEY,
        name              VARCHAR,
        location          VARCHAR,
        latitude          DECIMAL,
        longitude         DECIMAL
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time        TIMESTAMP   SORTKEY   DISTKEY,
        hour              INT,
        day               INT,
        week              INT,
        month             INT,
        year              INT,
        weekday           INT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role {}
    json {}
    region 'us-west-2';
""").format(LOG_DATA_PATH, ARN_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role {}
    json 'auto'
    region 'us-west-2';
""").format(SONG_DATA_PATH, ARN_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, \
        session_id, location, user_agent) \
    SELECT DISTINCT \
        TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
        se.userId       AS user_id,
        se.level        AS level,
        ss.song_id      AS song_id,
        ss.artist_id    AS artist_id,
        se.sessionId    AS session_id,
        se.location     AS location,
        se.userAgent    AS user_agent
    FROM staging_events AS se
        INNER JOIN staging_songs AS ss ON se.song = ss.title
            AND se.artist = ss.artist_name
            AND se.length = ss.duration
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId      AS user_id,
        firstName   AS first_name,
        lastName    AS last_name,
        gender      AS gender,
        level       AS level
    FROM staging_events
    WHERE page = 'NextSong'
        AND userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id         AS artist_id,
        artist_name       AS name,
        artist_location   AS location,
        artist_latitude   AS latitude,
        artist_longitude  AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT
        start_time, 
        extract(hour from start_time), 
        extract(day from start_time), 
        extract(week from start_time), 
        extract(month from start_time), 
        extract(year from start_time), 
        extract(weekday from start_time)
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
