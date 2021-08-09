from database.database import connect_db

# create connection to the sparkify database
session = connect_db(keyspace_name="sparkify")


