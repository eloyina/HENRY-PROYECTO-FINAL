import pandas as pd
import psycopg2
from psycopg2 import Error
print("Ingrese el nombre de la base de datos:")
database=input()
print("Ingrese su usuario:")
user = input()
print("Ingrese su contraseña:")
password = input()
print("Ingrese host:")
host = input()
try:
        #Conexión al servidor
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        #Crear nuestra base de datos
        cursor.execute("CREATE DATABASE greendata;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)