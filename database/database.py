from cassandra.cluster import Cluster


def connect_db(keyspace_name):
    """Connects to the locally deployed cassandra cluster

    Returns:
        cassandra.cluster.Session: Session of a connected keyspace
    """

    # connects to a local cluster
    try:
        cluster = Cluster(["127.0.0.1"])
        session = cluster.connect()
    except Exception as error:
        print(error)

    # drop keyspace if exists
    try:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_name}")
    except Exception as error:
        print("Unable to drop keyspace")
        print(error)

    # connect to the keyspace
    try:
        session.execute(
            f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
        WITH REPLICATION = 
        {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}"""
        )
    except Exception as error:
        print("Unable to create keyspace")
        print(error)

    try:
        session.set_keyspace(f"{keyspace_name}")
    except Exception as error:
        print("Unable to set keyspace")
        print(error)

    return session
