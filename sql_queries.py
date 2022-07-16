"""
Specify the schema from where the tables will be dropped. You must specify the same schema name
where tables where created.
"""
schema_name = "sparkify"

# DROP TABLES
"""
Construction of Drop Table DDL
"""
songplay_table_drop = f"DROP TABLE IF EXISTS {schema_name}.songplays"
user_table_drop = f"DROP TABLE IF EXISTS {schema_name}.users"
song_table_drop = f"DROP TABLE IF EXISTS {schema_name}.songs"
artist_table_drop = f"DROP TABLE IF EXISTS {schema_name}.artists"
time_table_drop = f"DROP TABLE IF EXISTS {schema_name}.time"

# CREATE TABLES

"""
Table: users - This DIMENSION table contains data of users in the music streaming app
"""
user_table_create = (f""" 
    CREATE TABLE IF NOT EXISTS {schema_name}.users
    (
    user_id integer NOT NULL,
    first_name varchar NOT NULL,
    last_name varchar NOT NULL,
    gender varchar NOT NULL,
    level varchar,
    CONSTRAINT user_id_pk PRIMARY KEY (user_id)
    ) TABLESPACE pg_default;
    
    ALTER TABLE IF EXISTS {schema_name}.users OWNER to student;
""")

"""
Table: artists  - This DIMENSION table contains detail about artists in music
"""
artist_table_create = (f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.artists
    (
        artist_id varchar NOT NULL,
        name varchar NOT NULL,
        location varchar,
        latitude varchar,
        longitude varchar,
        CONSTRAINT artist_id_pk PRIMARY KEY (artist_id)
    )
    TABLESPACE pg_default;
    
    ALTER TABLE IF EXISTS {schema_name}.artists OWNER to student;
""")

"""
Table: songs - This DIMENSION table contains detail information about songs
"""
song_table_create = (f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.songs
    (
    song_id varchar NOT NULL,
    title varchar,
    artist_id varchar,
    year integer,
    duration numeric,
    CONSTRAINT song_id_pk PRIMARY KEY (song_id),
    CONSTRAINT artist_id_fk FOREIGN KEY (artist_id)
    REFERENCES {schema_name}.artists (artist_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    ) 
    TABLESPACE pg_default;
            
    ALTER TABLE IF EXISTS {schema_name}.songs OWNER to student;
""")

"""
Table: time - This DIMENSION table contains timestamps of records in songplays broken down into specific time units
"""
time_table_create = (f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.time
    (
        start_time TIMESTAMP,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday integer        
    )
    TABLESPACE pg_default;
    
    ALTER TABLE IF EXISTS {schema_name}.time OWNER to student;
""")

"""
Table: songplays - This FACT table contains information about what song user play, when he played song, 
what is his subscription level etc.
"""
songplay_table_create = (f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.songplays
    (
    songplay_id SERIAL NOT NULL,
    start_time TIMESTAMP,
    user_id integer,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id integer,
    location varchar,
    user_agent varchar,
    CONSTRAINT songplay_id_pk PRIMARY KEY (songplay_id),
    CONSTRAINT artist_id_fk FOREIGN KEY (artist_id)
        REFERENCES {schema_name}.artists (artist_id) MATCH FULL
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT song_id_fk FOREIGN KEY (song_id)
        REFERENCES {schema_name}.songs (song_id) MATCH FULL
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT user_id_fk FOREIGN KEY (user_id)
        REFERENCES {schema_name}.users (user_id) MATCH FULL
        ON UPDATE NO ACTION
        ON DELETE NO ACTION    
    ) 
    TABLESPACE pg_default;
            
    ALTER TABLE IF EXISTS {schema_name}.songplays OWNER to student;
""")

# INSERT RECORDS
"""
The inserting of data must be in following order due to primary and foreign key constraint. Each dimension table has 
Primary key. Fact tables FKs are from dimension tables
"""
user_table_insert = (f""" 
INSERT INTO {schema_name}.users(user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (user_id)
DO NOTHING;
""")

artist_table_insert = (f"""
INSERT INTO {schema_name}.artists(artist_id, name, location, latitude, longitude) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO NOTHING;
""")

song_table_insert = (f"""
INSERT INTO {schema_name}.songs(song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO NOTHING;
""")

time_table_insert = (f"""
INSERT INTO {schema_name}.time(start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s);
""")

songplay_table_insert = (f"""
INSERT INTO {schema_name}.songplays(start_time, user_id, level, song_id,
artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")


# FIND SONGS

song_select = (f"""SELECT a.artist_id, s.song_id FROM {schema_name}.artists a LEFT JOIN  {schema_name}.songs s 
ON a.artist_id = s.artist_id WHERE s.title = %s AND a.name = %s  AND s.duration = %s""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create,
                        songplay_table_create]
drop_table_queries = [songplay_table_drop, song_table_drop, user_table_drop, artist_table_drop, time_table_drop]
