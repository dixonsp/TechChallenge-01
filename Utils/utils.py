import sqlite3
import glob
from pathlib import Path
from model.negocio import eTipo, eSubTipo
from decimal import Decimal, InvalidOperation

def conectar_sqlite():
        """ create a database connection to an SQLite database """
        conn = None
        filename = "data/sqlite/TechChallenge01.db"
        try:
            conn = sqlite3.connect(filename)
            print(sqlite3.sqlite_version)
        except sqlite3.Error as e:
            print(e)
        finally:
            if conn:
                return conn
            

