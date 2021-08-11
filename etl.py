from preprocess import preprocess
from database.database import Database
from queries import (
    song_info_by_session_select,
    song_info_by_user_select,
    user_info_by_song_select,
)

# preprocess the data
# preprocess()

db = Database()
# create connection to the sparkify database
cluster, session = db.connect_db(keyspace_name="sparkify")

# drop tables if exists
db.drop_tables(session=session)

# create tables
db.create_tables(session=session)


# insert values
db.insert_values(session=session, filepath="./event_datafile_new.csv")

# select values
# music library
df_music = db.select_values(session=session, query=song_info_by_session_select)
print(df_music.head())

# # user playlist info
df_user_playlist = db.select_values(session=session, query=song_info_by_user_select)
print(df_user_playlist.head())

# user_info_by_song
df_user_info_by_song = db.select_values(session=session, query=user_info_by_song_select)
print(df_user_info_by_song.head())
