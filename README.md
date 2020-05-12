## Project: Data Modeling With Postgres (1a)
#### Data Engineering Nanodegree
##### Student: Brian Pederson    
&nbsp;
#### Project Description
Build a dimensional star schema data model using Postgres consisting of one fact  and four dimensions. Write a basic ETL pipeline that transfers data from json files in two local directories into these tables using Python and SQL. 

##### Data Sources (json files)
- songdata - relative filepath: data/song_data
- logdata - relative filepath: data/log_data

##### Data Targets (Data Warehouse Tables)
- songplay - fact table
- users - dimension table
- time - dimension table
- songs - dimension table
- artists - dimension table

##### Program files
- sql_queries.py - Script encapsulating all SQL statements.  
- create_tables.py - Script to initialize environment by creating database and tables.
- etl.py - Script to implement simple ETL processes for DWH tables

Note there are other files present in the project workspace used in development which are not technically part of the project submission.
- etl.ipynb - Jupyter notebook used to hack code for etl.py
- test.ipynb - Jupyter notebook used to test development
- troubleshoot.ipynb - Jupyter notebook used to debug various issues such as mismatch of song/artist data vs. log data.
- troubleshoot.py - Python script used to debug various issues such as mismatch of song/artist data vs. log data. 

##### How to run the project
The process assumes that test data is located in a data subdirectory with subdirectories song_data and log_data.
Execute create_tables.py to create dev database and DWH tables.
Execute etl.py to load data from two source directories into five DWH tables.

##### Miscellaneous Notes
1. The test data provided is poorly configured. In particular the log/event data is not matched against song/artist data. In a normal DWH the dimension data should match closely with the fact data. This is because it's normal to use inner joins when joining fact to dimension tables. The missing dimension song/artist data causes (near) empty results for any inner join query which includes those dimensions. One workaround would be to create dummy rows in the dimensions with a meaning of 'missing data'. I'm not doing that in this project but will do it in the following project using AWS Redshift. In my opinion the log/event data should have been generated based on the sample songs so that this artificial problem would not occur. For myself and others (judging from reading comments in "Knowledge") this caused a lot of confusion that was unnecessary.
2. I added an alternate function to process song data called process_song_file2. This function avoids using pandas to process the data using native Python functionality. Based on this I'm not sure why the course designers utilized pandas for this task. The non pandas version seems to run fine.
3. I chose to only implement updates for the users dimension since it contains several columns that could change over time including level which is likely to change. For songs, artists, and time the insert statements ignore duplicate rows since the original data for these is as justifiable as subsequent data.
4. At the suggestion of a reviewer I applied foreign key constraints on the songplays and songs tables. However I did leave the artist_id and song_id FK columns on songplays NULLable due to the test data situation mentioned in Note 1 above. I would not do that in a production environment. Instead I would create dummy rows in the dimensions and then set the FKs on the fact table to NOT NULL. I do that in the following AWS DWH project.
5. The design of the time dimension in this project is a little strange. It seems to have granularity down to the level of microseconds. I think in production the grain for this should be brought up to the second or even minute. As currently defined there is almost a one to one ratio between fact rows and time dimension rows. Normally dimensions should not grow as big as their accompanying fact tables.
