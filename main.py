from psycopg2 import connect
from constants import table_name, connection_params, new_users


def main():
    # declare connection instance
    conn = create_db_connection(connection_params)

    if conn is not None:
        conn.autocommit = True
        # Drop table if exists, create new table
        drop_and_create_new_table(conn, table_name)
        # Insert data
        insert_db_data_array(conn, new_users, table_name)
        # close connection
        conn.close()
    else:
        print("Error. Operation was not executed")


def drop_and_create_new_table(connection, table):
    # declare a cursor object from the connection
    cur = connection.cursor()
    cur.execute(f"DROP TABLE IF EXISTS  {table} CASCADE")
    print(f"Table '{table}' dropped")
    cur.execute(f"CREATE TABLE {table} "
                f"(id INT PRIMARY KEY NOT NULL, name TEXT NOT NULL, surname TEXT NOT NULL, age INT);"
                f"")
    print(f"Table '{table}' created")
    cur.close()


def insert_db_data_array(connection, user_data, table):
    for index, item in enumerate(user_data):
        insert_data_row(connection, table, index, item)


def show_db_data(connection, table):
    # declare a cursor object from the connection
    cur = connection.cursor()
    cur.execute(f"SELECT * FROM {table};")

    # enumerate() over the PostgreSQL records
    for i, record in enumerate(cur):
        print("\n", type(record))
        print(record)

    show_db_data(connection)
    cur.close()


def insert_data_row(connection, table, item_index, item_object):
    # declare a cursor object from the connection
    cur = connection.cursor()

    # noinspection PyBroadException
    try:
        cur.execute(
            f"INSERT INTO {table}(id, name, surname, age) "
            f"VALUES ({item_index}, '{item_object['name']}', '{item_object['surname']}', {item_object['age']});"
        )
        print(
            f"{item_index}. name: {item_object['name']} surname: {item_object['surname']} age: {item_object['age']}"
            f" SUCCESSFULLY ADDED")
    except Exception:
        print("Unable to execute command", Exception)
    finally:
        cur.close()


def create_db_connection(params):
    connection = None

    # noinspection PyBroadException
    try:
        connection = connect(
            dbname=params["dbname"],
            user=params["user"],
            password=params["password"],
            host=params["host"],
            port=params["port"]
        )
    except Exception:
        print("Unable to execute command", Exception)
        connection = None
    finally:
        return connection


if __name__ == "__main__":
    # execute only if run as a script
    main()
