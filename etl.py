import csv
from preprocess import preprocess
from database.database import Database
from queries import music_library_select, user_playlist_select, user_info_select

# preprocess the data
preprocess()

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
df_music = db.select_values(session=session, query=music_library_select)
print(df_music.head())

# # user playlist info
df_user_playlist = db.select_values(session=session, query=user_playlist_select)
print(df_user_playlist.head())

# user_info
df_user_info = db.select_values(session=session, query=user_info_select)
print(df_user_info.head())
