import pandas as pd
import psycopg2
from psycopg2 import Error
print("Ingrese el nombre de la base de datos:")
database= "postgres"
print("Ingrese su usuario:")
user = 'pf10@pf10sql'
print("Ingrese su contraseña:")
password = "!123henryfinal"
print("Ingrese host:")
host = 'pf10sql.postgres.database.azure.com'
try:
        #Conexión al servidor
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        #Crear nuestra base de datos
        cursor.execute("CREATE DATABASE greendata5;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)