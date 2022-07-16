1.Sparkify Project Objective
   
  - To design and create a PostgreSQL database, schema and tables.
  - Store artists, songs, users, time and songsplay data in respective tables to optimize queries on song play.
  - The Sparkify analytics team can perform analytics on stored data easily to understand what songs users are listening to on music streaming app.

2.How run the project scripts?

 You can run the project each file create_tables.py and etl.py respectively to test. But for simplicity of testing
 I have created run_all.py to run the both script in single go in respective order.
 
 a.Using DOS/Annaconda Prompt
 - Open command any of the command Prompt.
 - Enter into the project directory “ProjectDM01" 
   cd <Path to directory ProjectDM01>
 - Once you are inside directory ProjectDM01 type : python run_all.py
 
 b.Using Pycharm IDE
 - Lunch the PyCharm IDE and then Open the project.
 - From Project folder open the run_all.py file.
 - Click on run menu and then run_all.
 
3.Explanation of the files in the repository

   - create_conn.py : Common script make database connection and return cursor and connection context. This requires inputting host name, user and password.
   
   - create_tables.py : Script to create database[sparkifydb], schema[sparkify] and tables[artists,songs,users,time,songplays]. 
   
   - etl.py : Script to process 'data/song_data' and data/log_data' and insert data into respective RDBMS tables.
   
   - sql_queries.py : Script contaiting all DDLs for DROP & CREATE and INSERT records and READ records.
   - etl.ipynb  : Interactive notebook for devleloping etl job.
   - test.ipynb : Testing notebook.
   - run_all.py : Sscript to run all the project related scripts in single go in respective order
   - sparkifydb.png : Database schema diagram

4. Database schema design and ETL pipeline.

   After analysing song and log data and keeping in mind analytical goal it was very obvious that I need to design "Start Schema" 
   
   From song data JSON file, it is very obvious that we need to organized artists and songs dimension data. This help in removing data redundancy and normalizing the data.
   
   The same case with log data JSON file. We need to organize it log data into users, time dimension.   
  
   Design fact and dimension tables for a star schema to analyze user song play activities on music streaming app.
   
   4.1. Fact Table
   4.1.1. song plays - records in log data associated with song plays i.e. records with page NextSong
	      songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

   4.2. Dimension Tables
   4.2.1.users - users in the app : user_id, first_name, last_name, gender, level
   4.2.2.songs - songs in music database :song_id, title, artist_id, year, duration
   4.2.3.artists - artists in music database :	artist_id, name, location, latitude, longitude
   4.2.4.time - timestamps of records in songplays broken down into specific units : start_time, hour, day, week, month, year, weekday

   4.3.	ETL Pipeline
	   Write an ETL pipeline that read song and JSON data, process them and insert data in respective tables in Postgres using Python and SQL.
   
5. Example queries for song play analysis

	SELECT DISTINCT u.user_id, u.first_name, u.last_name, s.title, sp.location, sp.user_agent
	  FROM sparkify.users u 
	  INNER JOIN sparkify.songplays sp ON u.user_id = sp.user_id
	  INNER JOIN sparkify.songs s ON sp.song_id = s.song_id
6.  Dashboard for analytic queries
    
	SELECT * FROM sparkify.user_song_play_dashboard
	
	Insert data using the COPY command to bulk insert log files instead of using INSERT on one row at a time