# create table queries
"""
Reason of selecting these primary key in song_info_by_session are
- Session_id and item_in_session is taken as first and second
item since these are used in where clause
- The song_title has been used to maintain the uniqueness of primary key
because it is more likely to have duplicate primary key in case of pallandrom number.
- Example: primaray key for session_id: 12 & item_in_session_id: 1 and session_id: 1
& item_in_session_id: 21 will be same
"""
song_info_by_session_create = """
CREATE TABLE IF NOT EXISTS song_info_by_session(
session_id int,
item_in_session int,
artist text,
song_title text,
length decimal,
PRIMARY KEY(session_id, item_in_session, song_title)
)
"""

"""
Reason of using this these primary key in song_info_by_user are
- user_id and session_id is taken as first and second
item since these are used in where clause
- The item_in_session has been used as clustering columns for sorting.
- song_title has been used to avoid the duplicate key scenerio for pallandrome numbers
"""
song_info_by_user_create = """
CREATE TABLE IF NOT EXISTS song_info_by_user(
user_id int,
username text,
session_id int,
item_in_session int,
artist text,
song_title text,
PRIMARY KEY(user_id, session_id, item_in_session, song_title)
)
"""

"""
Reason of using this these primary key in user_info_by_song are
- song_title is taken as first item since this is used in where clause
- The username column is used to maintain the uniqueness in primary key.
"""
user_info_by_song_create = """
CREATE TABLE IF NOT EXISTS user_info_by_song(
username text,
gender text,
level text,
location text,
song_title text,
PRIMARY KEY(song_title, username)
)
"""

# insert table queries
song_info_by_session_insert = """
INSERT INTO song_info_by_session(
session_id, item_in_session, artist, song_title, length
)
VALUES(%s, %s, %s, %s, %s)
"""

song_info_by_user_insert = """
INSERT INTO song_info_by_user(
user_id, username, session_id, item_in_session, artist, song_title
)
VALUES(%s, %s, %s, %s, %s, %s)
"""

user_info_by_song_insert = """
INSERT INTO user_info_by_song(
username, gender, level, location, song_title    
)
VALUES(%s, %s, %s, %s, %s)
"""

# drop table queries
song_info_by_session_drop = "DROP TABLE IF EXISTS song_info_by_session"
song_info_by_user_drop = "DROP TABLE IF EXISTS song_info_by_user"
user_info_by_song_drop = "DROP TABLE IF EXISTS user_info_by_song"


# Query 1.
# Give me the artist, song title and song's length in the music app history
# that was heard during sessionId = 338, and itemInSession  = 4

song_info_by_session_select = """
SELECT artist, song_title, length
FROM song_info_by_session
WHERE session_id = 338 AND item_in_session = 4
"""

# Query 2.
# Give me only the following: name of artist, song (sorted by itemInSession)
# and user (first and last name) for userid = 10, sessionid = 182
song_info_by_user_select = """
SELECT artist, song_title, username
FROM song_info_by_user
WHERE user_id=10 and session_id=182
"""

# Query 3.
# Give me every user name (first and last) in my music app history
# who listened to the song 'All Hands Against His Own'
user_info_by_song_select = """
SELECT username
FROM user_info_by_song
WHERE song_title='All Hands Against His Own'
"""
