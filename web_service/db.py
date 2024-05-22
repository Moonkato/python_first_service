import psycopg2
from psycopg2 import sql

connector = psycopg2.connect(dbname="postgres", user="postgres", password="2552Moonkato", host="127.0.0.1")
cursor = connector.cursor()

connector.autocommit = True

# cursor.execute('''SELECT 1 FROM pg_database WHERE datname = %s;''', ('people',))
# exists = cursor.fetchone()
#
# if exists:
#     cursor.execute(sql.SQL('''DROP DATABASE {};''').format(sql.Identifier('people')))
#     print("Database is dropped")
#     cursor.execute("CREATE DATABASE people")
# else:
#     cursor.execute("CREATE DATABASE people")

connector = psycopg2.connect(dbname="people", user="postgres", password="2552Moonkato", host="127.0.0.1")
cursor = connector.cursor()