import sqlite3

def create_sqlite_database(filename):
        """ create a database connection to an SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(filename)
            print(sqlite3.sqlite_version)
        except sqlite3.Error as e:
            print(e)
        finally:
            if conn:
                return conn
