# SONG PLAY ANALYSIS USING APACHE CASSANDRA

## Propose of the project
The propose of this project is to analyze the data collected by a music streaming app for better understanding of user listening behavior. Thus, the database has been designed to answer the following queries
-  Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
- Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
- Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

## Tech stack
- Python
- Apache Cassandra
- Notion for project management
- Github for Version control
- List of libraries are defined in the [libraries&#46;md](libraries.md) file

## Tables design
<img src="resources/table_design.jpg" width="500">


## Files description
The project consists of following files
### queries&#46;py
It consists of all database queries

### database&#46;py
This program performs operations such as:

- Establish connection with database
- Drops table 
- Creates table
- Insert values
- Select values

### etl&#46;py & etl.ipynp
Both of the program perform same actions: 
- Preprocess the event data
- Store the data in the form of tables
- Perform select operation on the database to answer the question

## Project management
To view the development of the project please visit this [link](https://bejewled-kip-9a9.notion.site/412e911f64fb451f97c0ac53a525ad1d?v=8a687efefa6d415f9d04fa4cab4ae713)
