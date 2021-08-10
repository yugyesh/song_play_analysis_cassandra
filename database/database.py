import csv

import pandas as pd
from cassandra.cluster import Cluster
from queries import (
    music_library_create,
    music_library_drop,
    music_library_insert,
    user_info_create,
    user_info_drop,
    user_info_insert,
    user_playlist_create,
    user_playlist_drop,
    user_playlist_insert,
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
            session.execute(music_library_create)
        except Exception as error:
            print("Unable to create music_library")
            print(error)

        # create user table
        try:
            session.execute(user_playlist_create)
        except Exception as error:
            print("Unable to create user_info")
            print(error)

        # create users
        try:
            session.execute(user_info_create)
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
                    # insert data into the music_library
                    session.execute(
                        music_library_insert,
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

                # insert data into the user_playlist insert
                try:
                    session.execute(
                        user_playlist_insert,
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

                # insert data to user_info table
                try:
                    session.execute(
                        user_info_insert,
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
            session.execute(music_library_drop)
        except Exception as error:
            print(error)

        # drop user table
        try:
            session.execute(user_playlist_drop)
        except Exception as error:
            print(error)

        # drop users table
        try:
            session.execute(user_info_drop)
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
