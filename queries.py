# create table queries
"""
Reason of selecting these primary key in music_library are
- Session_id and item_in_session is taken as first and second
item since these are used in where clause
- The song_title has been used to maintain the uniqueness of primary key
because it is more likely to have duplicate primary key in case of pallandrom number.
- Example: primaray key for session_id: 12 & item_in_session_id: 1 and session_id: 1
& item_in_session_id: 21 will be same
"""
music_library_create = """
CREATE TABLE IF NOT EXISTS music_library(
session_id int,
item_in_session int,
artist text,
song_title text,
length decimal,
PRIMARY KEY(session_id, item_in_session, song_title)
)
"""

"""
Reason of using this these primary key in user_playlist are
- user_id and session_id is taken as first and second
item since these are used in where clause
- The item_in_session has been used as clustering columns for sorting.
- song_title has been used to avoid the duplicate key scenerio for pallandrome numbers
"""
user_playlist_create = """
CREATE TABLE IF NOT EXISTS user_playlist(
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
Reason of using this these primary key in user_info are
- song_title is taken as first item since this is used in where clause
- The username column is used to maintain the uniqueness in primary key.
"""
user_info_create = """
CREATE TABLE IF NOT EXISTS user_info(
username text,
gender text,
level text,
location text,
song_title text,
PRIMARY KEY(song_title, username)
)
"""

# insert table queries
music_library_insert = """
INSERT INTO music_library(
session_id, item_in_session, artist, song_title, length
)
VALUES(%s, %s, %s, %s, %s)
"""

user_playlist_insert = """
INSERT INTO user_playlist(
user_id, username, session_id, item_in_session, artist, song_title
)
VALUES(%s, %s, %s, %s, %s, %s)
"""

user_info_insert = """
INSERT INTO user_info(
username, gender, level, location, song_title    
)
VALUES(%s, %s, %s, %s, %s)
"""
