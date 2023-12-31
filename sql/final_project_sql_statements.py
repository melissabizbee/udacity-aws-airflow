class sqlqueries:
    songplay_table_insert = ("""
      INSERT INTO songplays (songplay_id, start_time, userid, level, song_id, artist_id, sessionid, location, useragent)
        SELECT
            md5(events.sessionid || events.ts) songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM (
            SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong' 
            AND sessionid IS NOT NULL 
            AND ts IS NOT NULL
        ) events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
        WHERE NOT EXISTS (
            SELECT 1 
            FROM songplays 
            WHERE songplay_id = md5(events.sessionid || events.ts)
        )

    """)

    user_table_insert = ("""
        INSERT INTO users (userid, first_name, last_name, gender, level)
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
        AND userid IS NOT NULL
    """)

    song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
            extract(month from start_time), extract(year from start_time), to_char(start_time, 'Day')
        FROM songplays
    """)

