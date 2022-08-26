import pandas as pd
import psycopg2
import os
from psycopg2 import Error

database = 'greendata'
user = 'pf10@pf10sql'
password = '!123henryfinal'
host = 'pf10sql.postgres.database.azure.com'
port = '5432'

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        print("Connection established")

        cursor = conn.cursor()
        cursor.execute("CREATE TABLE inventory (id serial PRIMARY KEY, name VARCHAR(50), quantity INTEGER);")
        cursor.execute("INSERT INTO inventory (name, quantity) VALUES (%s, %s);", ("banana", 150))

        print("Finished creating table")
        query =("CREATE TABLE inventory (id serial PRIMARY KEY, name VARCHAR(50), quantity INTEGER);")
        #"INSERT INTO inventory (name, quantity) VALUES ('banana', 150);"

        #"SELECT * FROM inventory;"
        #"CREATE TABLE inventory (id serial PRIMARY KEY, name VARCHAR(50),
        #quantity INTEGER);" #inserte el query
        """
        cursor.execute("INSERT INTO inventory (name, quantity) VALUES (%s, %s);", ("banana", 150))
        cursor.execute("INSERT INTO inventory (name, quantity) VALUES (%s, %s);", ("orange", 154))
        cursor.execute("INSERT INTO inventory (name, quantity) VALUES (%s, %s);", ("apple", 100))
        print("Inserted 3 rows of data")
        cursor.execute("SELECT * FROM inventory;")
        rows = cursor.fetchall()
        cursor.execute("SELECT * FROM inventory;")
        rows = cursor.fetchall()
        conn.commit()
        print("logre llegar aui")
        """
        cursor.execute("SELECT * FROM inventory WHERE name='apple';")
        rows = cursor.fetchall()

# Print all rows

        for row in rows:
            print("Data row = (%s, %s, %s)" %(str(row[0]), str(row[1]), str(row[2])))
except (Exception, psycopg2.Error) as error:
        print(error)
