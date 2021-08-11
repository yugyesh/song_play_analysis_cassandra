import csv

import pandas as pd
from cassandra.cluster import Cluster
from queries import (
    song_info_by_session_create,
    song_info_by_session_drop,
    song_info_by_session_insert,
    user_info_by_song_create,
    user_info_by_song_drop,
    user_info_by_song_insert,
    song_info_by_user_create,
    song_info_by_user_drop,
    song_info_by_user_insert,
)


class Database:
    def __init__(self) -> None:
        pass

    def connect_db(self, keyspace_name):
        """Connects to the locally deployed cassandra cluster

        Returns:
            cassandra.cluster.Session: Session of a connected keyspace
        """

        # connects to a local cluster
        try:
            self.cluster = Cluster(["127.0.0.1"])
            self.session = self.cluster.connect()
        except Exception as error:
            print(error)

        # drop keyspace if exists
        try:
            self.session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_name}")
        except Exception as error:
            print("Unable to drop keyspace")
            print(error)

        # connect to the keyspace
        try:
            self.session.execute(
                f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
            WITH REPLICATION = 
            {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}"""
            )
        except Exception as error:
            print("Unable to create keyspace")
            print(error)

        try:
            self.session.set_keyspace(f"{keyspace_name}")
        except Exception as error:
            print("Unable to set keyspace")
            print(error)

        return self.cluster, self.session

    def create_tables(self, session):
        """Creates all tables

        Args:
            cassandra.cluster.Session: Session of a connected keyspace
        """
        # create music library table
        try:
            session.execute(song_info_by_session_create)
        except Exception as error:
            print("Unable to create song_info_by_session")
            print(error)

        # create song_info_by_user table
        try:
            session.execute(song_info_by_user_create)
        except Exception as error:
            print("Unable to create user_info_by_song")
            print(error)

        # create user_info_by_song table
        try:
            session.execute(user_info_by_song_create)
        except Exception as error:
            print(error)

    def insert_values(self, session, filepath):
        """Inserts value to all tables

        Args:
            cassandra.cluster.Session: Session of a connected keyspace
        """
        with open(filepath, encoding="utf8") as f:
            csv_file = csv.DictReader(f)

            for row in csv_file:
                try:
                    # insert data into the song_info_by_session
                    session.execute(
                        song_info_by_session_insert,
                        (
                            int(row["sessionId"]),
                            int(row["itemInSession"]),
                            row["artist"],
                            row["song"],
                            float(row["length"]),
                        ),
                    )
                except Exception as error:
                    print(error)

                # insert data into the song_info_by_user insert
                try:
                    session.execute(
                        song_info_by_user_insert,
                        (
                            int(row["userId"]),
                            f"{row['firstName']} {row['lastName']}",
                            int(row["sessionId"]),
                            int(row["itemInSession"]),
                            row["artist"],
                            row["song"],
                        ),
                    )
                except KeyError as error:
                    print("Key error")
                    print(error)
                except Exception as error:
                    print("Error inserting user playlist")
                    print(error)

                # insert data to user_info_by_song table
                try:
                    session.execute(
                        user_info_by_song_insert,
                        (
                            f"{row['firstName']} {row['lastName']}",
                            row["gender"],
                            row["level"],
                            row["location"],
                            row["song"],
                        ),
                    )
                except KeyError as error:
                    print("Key error")
                    print(error)
                except Exception as error:
                    print("Error inserting user info")
                    print(error)

    def drop_tables(self, session):
        """Drop all tables

        Args:
            cassandra.cluster.Session: Session of a connected keyspace
        """
        # drop music library table
        try:
            session.execute(song_info_by_session_drop)
        except Exception as error:
            print(error)

        # drop user table
        try:
            session.execute(song_info_by_user_drop)
        except Exception as error:
            print(error)

        # drop users table
        try:
            session.execute(user_info_by_song_drop)
        except Exception as error:
            print(error)

    def pandas_factory(self, colnames, rows):
        return pd.DataFrame(rows, columns=colnames)

    def select_values(self, session, query):
        try:
            session.row_factory = self.pandas_factory
            rows = session.execute(query)
            df = rows._current_rows
            return df
        except Exception as error:
            print("Unable to select the data")
            print(error)
