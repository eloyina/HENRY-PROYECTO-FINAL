import pandas as pd
import psycopg2
import os
from psycopg2 import Error
import sqlalchemy


#########################################################
#              Conexión con Postgres                    #
#########################################################

print("Ingrese el nombre de la base de datos:")
database=input()
print("Ingrese su usuario:")
user = input()
print("Ingrese su contraseña:")
password = input()
print("Ingrese host:")
host = input()

#Busqueda de archivos a partir de una ruta y creación de variables
print("Seleccione la ruta donde se encuentran sus datasets:")
carpeta = input()
lista_datos = []
for dataset in os.listdir(carpeta):
    lista_datos.append(dataset)
archivo0=carpeta+"\\"+lista_datos[0]
archivo1=carpeta+"\\"+lista_datos[1]
archivo2=carpeta+"\\"+lista_datos[2]
archivo3=carpeta+"\\"+lista_datos[3]
archivo4=carpeta+"\\"+lista_datos[4]
archivo5=carpeta+"\\"+lista_datos[5]
archivo6=carpeta+"\\"+lista_datos[6]
archivo7=carpeta+"\\"+lista_datos[7]

#########################################################
#                       ETL                             #
#########################################################

# Tabla localización
loc_global = pd.DataFrame(pd.read_csv(archivo5))
mundo = pd.DataFrame(pd.read_csv(archivo7))
loc_global = loc_global[['country','iso_code']].drop_duplicates().reset_index(drop=True)
loc_global[loc_global['country']=='World']= loc_global[loc_global['country']=='World'].fillna("WRL")
loc_global.rename(columns={'country': 'name'},inplace=True, errors='raise')
loc_global.dropna(inplace=True)
loc_final = loc_global.merge(mundo, on='name', how='left')
loc_final = loc_final[["iso_code","name","region"]]
loc_final.rename(columns={'iso_code':"id_pais", "name":"pais", "region": "region"}, inplace=True)
loc_final[loc_final['pais']=='World']= loc_final[loc_final['pais']=='World'].fillna("World")
loc_final = loc_final.append({"id_pais":"KOS", "pais":"Kosovo", "region":"Europe"}, ignore_index=True)
loc_final['id_pais'].dropna(inplace=True)
loc_region = pd.DataFrame(loc_final["region"])
loc_region.drop_duplicates(inplace=True)
loc_region.reset_index(inplace= True, drop=True)
loc_region.reset_index(inplace= True, drop=False)
loc_region.rename(columns={"index":"id_region"}, inplace=True)
loc_pais = loc_final[["id_pais","pais"]]
loc_pais.reset_index(inplace=True, drop=True)
loc_pais = loc_pais.merge(loc_final, on=["pais","id_pais"], how="left")
loc_pais =loc_pais.merge(loc_region, on=["region"], how= "left")
loc_pais.reindex(columns=["id_pais","id_region","pais", "region"])
loc_region.loc[loc_region['id_region']==5,'region']='Otros'

# Tabla aniopais
clave_aniopais = pd.DataFrame(pd.read_csv(archivo5))
clave_aniopais = clave_aniopais.rename(columns={"year":"anio", "iso_code": "id_pais", "country":"pais"})
clave_aniopais = clave_aniopais[["anio","id_pais","pais"]]
clave_aniopais[clave_aniopais['pais']=='World'] = clave_aniopais[clave_aniopais['pais']=='World'].fillna("WRL")
clave_aniopais.merge(loc_pais, on="pais", how="left")
clave_aniopais = clave_aniopais[clave_aniopais["anio"] >= 1980]
clave_aniopais.insert(0, "id_aniopais", clave_aniopais["id_pais"] +"-"+ clave_aniopais["anio"].map(str))
clave_aniopais.dropna(inplace=True)
clave_aniopais.reset_index(inplace=True,drop=True)

# Tablas producción mundo y paises
produccion = pd.DataFrame(pd.read_csv(archivo5))
produccion = produccion[["year", "country", "coal_prod_change_pct","coal_production", "gas_prod_change_pct", "gas_production", "oil_prod_change_pct", "oil_production"]]
produccion = produccion[produccion["year"] >= 1980]
produccion = produccion.rename(columns={"year":"anio", "country":"pais","iso_code":"id_Pais", "coal_production": "prod_carbon", "coal_prod_change_pct": "prod_carb_porc_anual","gas_production":"prod_gas", "gas_prod_change_pct":"prod_gas_porc_anual", "oil_production": "prod_petroleo", "oil_prod_change_pct": "prod_petr_porc_anual"})
produccion = produccion.merge(loc_pais, how="right", on="pais")
produccion = produccion.merge(clave_aniopais, how="right", on=["pais","anio"])
produccion.insert(0, "id_prod", "P-" + produccion["id_aniopais"])
produccion_mundo = produccion[produccion["pais"]=="World"]
produccion_paises = produccion[produccion["pais"]!="World"]
produccion_mundo.drop(columns=["id_aniopais", "pais", "id_pais_x"], inplace=True)
produccion_mundo.rename(columns={"id_pais_y":"id_pais"}, inplace= True)
produccion_mundo = produccion_mundo.reindex(columns=["id_prod",	"id_pais","prod_carb_porc_anual","prod_carbon","prod_gas_porc_anual","prod_gas","prod_petr_porc_anual","prod_petroleo"])
produccion_mundo.reset_index(inplace=True, drop=True)
p = produccion_paises.fillna(0)
produccion_paises.drop(p[p["id_prod"]==0].index, inplace=True)
produccion_paises.drop(columns=["id_aniopais", "pais", "id_pais_x"], inplace=True)
produccion_paises.rename(columns={"id_pais_y":"id_pais"}, inplace= True)
produccion_paises = produccion_paises.reindex(columns=["id_prod","id_pais","prod_carb_porc_anual","prod_carbon","prod_gas_porc_anual","prod_gas","prod_petr_porc_anual","prod_petroleo"])
produccion_paises.reset_index(inplace=True, drop=True)

# Tablas consumos mundo y paises
consumos = pd.DataFrame(pd.read_csv(archivo5))
consumos = consumos[["year", "country", "iso_code", "biofuel_cons_change_pct", "biofuel_consumption", "coal_cons_change_pct", "coal_consumption", "coal_share_energy", "gas_cons_change_pct","gas_consumption", "hydro_cons_change_pct", "hydro_consumption", "nuclear_cons_change_pct", "nuclear_consumption", "oil_cons_change_pct", "oil_consumption", "solar_cons_change_pct", "solar_consumption", "wind_cons_change_pct", "wind_consumption"]]
consumos = consumos[consumos["year"] >= 1980]
consumos = consumos.rename(columns={"year":"anio","country":"pais","iso_code":"id_pais", "biofuel_cons_change_pct": "con_bio_porc_anual", "biofuel_consumption": "con_biocomb", "coal_cons_change_pct":"con_carb_porc_anual", "coal_consumption": "con_carbon","coal_share_energy":"en_con_carb_porc_anual", "gas_cons_change_pct":"con_gas_porc_anual", "gas_consumption":"con_gas", "hydro_cons_change_pct":"con_hydro_porc_anual", "hydro_consumption":"con_hydro","nuclear_cons_change_pct":"con_nuc_porc_anual", "nuclear_consumption":"con_nuc", "oil_cons_change_pct":"con_petr_porc_anual", "oil_consumption":"con_petr", "solar_cons_change_pct":"con_sol_porc_anual", "solar_consumption":"con_sol", "wind_cons_change_pct":"con_eol_porc_anual", "wind_consumption":"con_eol"})
consumos = consumos.merge(loc_pais, how="right", on=["pais"])
consumos = consumos.merge(clave_aniopais, how="right", on=["pais","anio"])
consumos.insert(0, "id_cons", "C-" + consumos["id_aniopais"])
consumos_mundo = consumos[consumos["pais"]=="World"]
consumos_paises = consumos[consumos["pais"]!="World"]
consumos_mundo.drop(columns=["anio", "id_aniopais", "pais", "id_pais_x", "id_pais"], inplace=True)
consumos_mundo.rename(columns={"id_pais_y":"id_pais"}, inplace= True)
consumos_mundo = consumos_mundo.reindex(columns=["id_cons","id_pais","con_bio_porc_anual","con_biocomb","con_carb_porc_anual","con_carbon","en_con_carb_porc_anual","con_gas_porc_anual","con_gas","con_hydro_porc_anual","con_hydro","con_nuc_porc_anual","con_nuc","con_petr_porc_anual","con_petr","con_sol_porc_anual","con_sol","con_eol_porc_anual","con_eol"])
consumos_mundo.reset_index(inplace=True, drop=True)
c = consumos.fillna(0)
consumos_paises.drop(c[c["id_cons"]==0].index, inplace=True)
consumos_paises.drop(columns=["anio", "id_aniopais", "pais", "id_pais_x", "id_pais_y"], inplace=True)
consumos_paises = consumos_paises.reindex(columns=["id_cons","id_pais","con_bio_porc_anual","con_biocomb","con_carb_porc_anual","con_carbon","en_con_carb_porc_anual","con_gas_porc_anual","con_gas","con_hydro_porc_anual","con_hydro","con_nuc_porc_anual","con_nuc","con_petr_porc_anual","con_petr","con_sol_porc_anual","con_sol","con_eol_porc_anual","con_eol"])
consumos_paises.reset_index(inplace=True, drop=True)

# Tablas generación mundo y paises
generacion = pd.DataFrame(pd.read_csv(archivo5))
generacion = generacion[["country","year","iso_code","population","gdp","biofuel_electricity","biofuel_share_elec","coal_electricity","coal_share_elec","gas_electricity","gas_share_elec","hydro_electricity","hydro_share_elec","net_elec_imports","net_elec_imports_share_demand","nuclear_electricity","nuclear_share_elec","oil_electricity","oil_share_elec","solar_electricity","solar_share_elec","wind_electricity","wind_share_elec"]]
generacion = generacion[generacion["year"]>=1980]
generacion.rename(columns={"country":"pais","year":"anio","iso_code":"id_Pais","population":"poblacion","gdp":"pbi","biofuel_electricity":"gen_biocomb","biofuel_share_elec":"gen_bio_porc","coal_electricity":"gen_carbon","coal_share_elec":"gen_carbon_porc","gas_electricity":"gen_gas","gas_share_elec":"gen_gas_porc","hydro_electricity":"gen_hidro","hydro_share_elec":"gen_hidro_porc","net_elec_imports":"import_netas","net_elec_imports_share_demand":"import_netas_porc","nuclear_electricity":"gen_nuclear","nuclear_share_elec":"gen_nuclear_porc","oil_electricity":"gen_petroleo","oil_share_elec":"gen_petroleo_porc","solar_electricity":"gen_solar","solar_share_elec":"gen_solar_porc","wind_electricity":"gen_eolica","wind_share_elec":"gen_eolica_porc"},inplace=True)
generacion = generacion.merge(loc_pais, how="right", on="pais")
generacion = generacion.merge(clave_aniopais, how="right", on=["pais","anio"])
generacion.insert(0, "id_gen", "G-" + generacion["id_aniopais"])
generacion_mundo = generacion[generacion["pais"]=="World"]
generacion_paises = generacion[generacion["pais"]!="World"]
generacion_mundo.drop(columns=["anio", "id_Pais", "id_aniopais", "pais", "id_pais_x"], inplace=True)
generacion_mundo.rename(columns={"id_pais_y":"id_pais"}, inplace= True)
generacion_mundo = generacion_mundo.reindex(columns=["id_gen","id_pais","poblacion","pbi","gen_biocomb","gen_bio_porc","gen_carbon","gen_carbon_porc","gen_gas","gen_gas_porc","gen_hidro","gen_hidro_porc","import_netas","import_netas_porc","gen_nuclear","gen_nuclear_porc","gen_petroleo","gen_petroleo_porc","gen_solar","gen_solar_porc","gen_eolica","gen_eolica_porc"])
generacion_mundo.reset_index(inplace=True, drop=True)
g = generacion.fillna(0)
generacion_paises.drop(g[g["id_gen"]==0].index, inplace=True)
generacion_paises.drop(columns=["anio", "id_Pais", "id_aniopais", "pais", "id_pais_x"], inplace=True)
generacion_paises.rename(columns={"id_pais_y":"id_pais"}, inplace= True)
generacion_paises = generacion_paises.reindex(columns=["id_gen","id_pais","poblacion","pbi","gen_biocomb","gen_bio_porc","gen_carbon","gen_carbon_porc","gen_gas","gen_gas_porc","gen_hidro","gen_hidro_porc","import_netas","import_netas_porc","gen_nuclear","gen_nuclear_porc","gen_petroleo","gen_petroleo_porc","gen_solar","gen_solar_porc","gen_eolica","gen_eolica_porc"])
generacion_paises.reset_index(inplace=True, drop=True)


# Tabla misiones de CO2 por pais
emision_co2 = pd.DataFrame(pd.read_csv(archivo1))
emision_co2=emision_co2[['Year','Country','CO2_emission']]
emision_co2=emision_co2.groupby(['Year','Country'], as_index=False).sum()
energias = pd.DataFrame(pd.read_csv(archivo5))
energias=energias[['year','country','carbon_intensity_elec']]
emision_co2.rename(columns={'Year':'year','Country':'country', "CO2_emission":"co2_emission"},inplace=True)
emision_co2_paises = emision_co2.merge(energias, how='inner',on=['year','country'])
emision_co2_paises.rename(columns={'year':'anio','country':'pais','co2_emission':'emision_co2','carbon_intensity_elec':'intensidad_carbono'}, inplace=True)
emision_co2_paises = emision_co2_paises[emision_co2_paises["pais"]!="World"]
emision_co2_paises = emision_co2_paises.merge(loc_pais, how="left", on="pais")
emision_co2_paises = emision_co2_paises.merge(clave_aniopais, how="left", on=["pais","anio"])
emision_co2_paises.insert(0, "id_emision", "E-" + emision_co2_paises["id_aniopais"])
emision_co2_paises.drop(columns=["anio", "pais", "id_aniopais", "id_pais_x"], inplace=True)
emision_co2_paises.rename(columns={"id_pais_y":"id_pais"}, inplace=True)
emp = emision_co2_paises.fillna(0)
emision_co2_paises.drop(emp[emp["id_pais"]==0].index, inplace=True)
emision_co2_paises = emision_co2_paises.reindex(columns=["id_emision","id_pais","emision_co2", "intensidad_carbono"])

# Tabla mundial: creación de columnas
# Emisión CO2 mundial
emision_co2_mundial=emision_co2[emision_co2['country']=='World']
emision_co2_mundial.rename(columns={"year":"Year"}, inplace=True)
emision_co2_mundial=emision_co2_mundial.groupby('Year',as_index=False).sum()
emision_co2_mundial_final=emision_co2_mundial[['Year','co2_emission']]
# Variación masa glaciares
glaciares=pd.DataFrame(pd.read_csv(archivo2))
glaciares=glaciares[glaciares['Year']>=1980]
glaciares.drop(columns=['Number of observations'], inplace=True)
# Variación nivel del mar
nivel=pd.DataFrame(pd.read_csv(archivo4, delimiter=";"))
nivel=nivel[nivel['Year']>=1980]
nivel.reset_index(inplace=True,drop=True)
nivel['NOAA Adjusted Sea Level']=nivel['NOAA Adjusted Sea Level'].fillna(nivel['CSIRO Adjusted Sea Level'])
nivel['CSIRO Adjusted Sea Level']=nivel['CSIRO Adjusted Sea Level'].fillna(nivel['NOAA Adjusted Sea Level'])
nivel['Mean_level']=(nivel['CSIRO Adjusted Sea Level']+nivel['NOAA Adjusted Sea Level'])/2
nivel=nivel[['Year','Mean_level']]
# Intensidad de emisiones
energias_mundial=energias[energias['country']=='World']
energias_mundial=energias_mundial[energias_mundial['year']>=1980]
energias_mundial.reset_index(inplace=True,drop=True)
energias_mundial.rename(columns={"year":"Year"},inplace=True)
energias_mundial_filtrado=energias_mundial[['Year','carbon_intensity_elec']]
# Variación de temperatura
var_temp=pd.DataFrame(pd.read_csv(archivo6, delimiter=";"))
var_temp=var_temp[var_temp['Year']>=1980]
var_temp=var_temp.groupby('Year', as_index=False).mean()
aniopais_mundo=clave_aniopais[clave_aniopais['id_pais']=='WRL']
# Creacion tabla mundial
emision_mundial=energias_mundial_filtrado.merge(emision_co2_mundial_final,how='left',on='Year').merge(nivel,how='left',on='Year').merge(glaciares,how='left',on='Year').merge(var_temp,how='left',on='Year')
emision_mundial.rename(columns={'Year':'anio','Country':'pais','carbon_intensity_elec':'intensidad_carbono','co2_emission':'emision_co2','Mean_level':'nivel_mar','Mean cumulative mass balance':'perdida_acumulada_glaciares_porc','Mean':'variacion_temperatura'},inplace=True)
emision_mundial = emision_mundial.merge(aniopais_mundo, how="left", on=["anio"])
emision_mundial.insert(0, "id_emision", "E-" + emision_mundial["id_aniopais"])
emision_mundial = emision_mundial.reindex(columns=["id_emision","anio","id_pais", "intensidad_carbono", "emision_co2", "nivel_mar", "perdida_acumulada_glaciares_porc", "variacion_temperatura"])

# Tabla tipo de planta
cod_planta={'id_tipo_planta':['LC','Fos','Alm','Cog','Otr'],'tipo_planta':['Low_Carbon','Fosil','Almacenamiento','Cogeneracion','Otros']}
tipo_planta=pd.DataFrame(cod_planta)

# Tabla calidad del aire
calidad_aire=pd.DataFrame(pd.read_csv(archivo0,sep=';'))
calidad_aire=calidad_aire[['ISO3','WHO Country Name','City or Locality','Measurement Year','PM2.5 (?g/m3)','PM10 (?g/m3)','NO2 (?g/m3)']]
calidad_aire=calidad_aire.groupby(['WHO Country Name','Measurement Year'],as_index=False).sum()
calidad_aire.rename({'WHO Country Name':'pais','Measurement Year':'anio','PM2.5 (?g/m3)':'pm2_5_gm3','PM10 (?g/m3)':'pm10_gm3','NO2 (?g/m3)':'no2_gm3'},axis=1,inplace=True)
calidad_aire = calidad_aire.merge(loc_pais, how="left", on="pais")
calidad_aire = calidad_aire.merge(clave_aniopais, how="left", on=["pais","anio"])
calidad_aire[calidad_aire['pais']=='United Republic of Tanzania'] = calidad_aire[calidad_aire['pais']=='United Republic of Tanzania'].fillna("TZA")
calidad_aire[calidad_aire['pais']=='United States of America'] = calidad_aire[calidad_aire['pais']=='United States of America'].fillna("USA")
calidad_aire[calidad_aire['pais']=='Venezuela (Bolivarian Republic of)'] = calidad_aire[calidad_aire['pais']=='Venezuela (Bolivarian Republic of)'].fillna("VEN")
calidad_aire[calidad_aire['pais']=='Viet Nam'] = calidad_aire[calidad_aire['pais']=='Viet Nam'].fillna("VNM")
calidad_aire.insert(0, "id_cal_aire", "AIR-" + calidad_aire["id_pais_y"] + "-" + calidad_aire["anio"].map(str))
calidad_aire.drop(columns=["pais", "id_aniopais","id_pais_x",], inplace=True)
calidad_aire.rename(columns={"id_pais_y":"id_pais"}, inplace=True)
cal = calidad_aire.fillna(0)
calidad_aire.drop(cal[cal["id_pais"]==0].index, inplace=True)
calidad_aire = calidad_aire.reindex(columns=["id_cal_aire", "id_pais", "anio", "pm2_5_gm3","pm10_gm3","no2_gm3"])

# Tabla plantas de energia
plantas=pd.read_csv(archivo3)
planta_energia=pd.DataFrame(data=plantas[['country','country_long','name','capacity_mw','latitude','longitude','primary_fuel']])
planta_energia=planta_energia.rename(columns={'country':'id_pais','country_long':'pais','name':'nombre','capacity_mw':'capacidad_mw','latitude':'latitud','longitude':'longitud','primary_fuel':'combustible_primario'})
dicc_tipos={'Solar':'Low_Carbon','Wave and Tidal':'Low_Carbon','Hydro':'Low_Carbon','Waste':'Low_Carbon','Wind':'Low_Carbon',
            'Wave and tidal':'Low_Carbon','Geothermal':'Low_Carbon','Nuclear':'Low_Carbon','Biomass':'Low_Carbon',
            'Gas':'Fosil','Oil':'Fosil','Coal':'Fosil','Petcoke':'Fosil',
            'Storage':'Almacenamiento',
            'Cogeneration':'Cogeneracion',
            'Other':'Otros'}
planta_energia['tipo_planta']=planta_energia['combustible_primario'].map(dicc_tipos)
planta_energia=planta_energia.merge(tipo_planta, how='outer',on='tipo_planta')
planta_energia.drop(columns=['pais'],inplace=True)
planta_energia.reset_index(inplace=True,drop=False)
planta_energia.rename(columns={'index':'id_planta'},inplace=True)
planta_energia=planta_energia.reindex(['id_planta','id_pais','id_tipo_planta','nombre','combustible_primario','capacidad_mw','latitud','longitud'],axis=1)


#########################################################
#         Conexión de BD y carga de tablas              #
#########################################################

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        loc_pais.to_sql("loc_p_aux", con=engine, if_exists='replace',index=False)
        cursor.execute("CREATE TABLE IF NOT EXISTS loc_pais(Id_pais text PRIMARY KEY, Pais text, Region text, Id_region int);")
        cursor.execute("INSERT INTO loc_pais (Id_pais, Pais, Region, Id_region)\
            SELECT Id_pais, Pais, Region, Id_region \
               FROM (SELECT * FROM loc_p_aux\
            WHERE loc_p_aux.id_pais NOT IN (SELECT Id_pais FROM loc_pais)) as tabla;")
        cursor.execute("DROP TABLE if exists loc_p_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        loc_region.to_sql("loc_r_aux", con=engine, if_exists='replace',index=False)
        cursor.execute("CREATE TABLE IF NOT EXISTS loc_region(id_region int PRIMARY KEY, region text);")
        cursor.execute("INSERT INTO loc_region (id_region, region)\
            SELECT id_region, region \
               FROM (SELECT * FROM loc_r_aux\
            WHERE loc_r_aux.id_region NOT IN (SELECT id_region FROM loc_region)) as tabla;")
        cursor.execute("DROP TABLE if exists loc_r_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        clave_aniopais.to_sql("aniopais_aux", con=engine, if_exists='replace',index=False)
        cursor.execute("CREATE TABLE IF NOT EXISTS aniopais(id_aniopais text PRIMARY KEY, anio int, id_pais text, pais text);")
        cursor.execute("INSERT INTO aniopais (id_aniopais, anio,id_pais,pais )\
            SELECT id_aniopais, anio,id_pais,pais  \
               FROM (SELECT * FROM aniopais_aux\
            WHERE aniopais_aux.id_aniopais NOT IN (SELECT id_aniopais FROM aniopais)) as tabla;")
        cursor.execute("DROP TABLE if exists aniopais_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        produccion_mundo.to_sql("prod_mundial_aux", con=engine, if_exists='replace',index=False)
        cursor.execute("DROP TABLE if exists prod_mundial;")
        cursor.execute("CREATE TABLE IF NOT EXISTS prod_mundial(id_prod text PRIMARY KEY,id_pais text, prod_carb_porc_anual float, prod_carbon float, prod_gas_porc_anual float,prod_gas float,prod_petr_porc_anual float, prod_petroleo float);")
        cursor.execute("INSERT INTO prod_mundial (id_prod, id_pais, prod_carb_porc_anual,prod_carbon,prod_gas_porc_anual,prod_gas,prod_petr_porc_anual,prod_petroleo)\
            SELECT  id_prod, id_pais, prod_carb_porc_anual,prod_carbon,prod_gas_porc_anual,prod_gas,prod_petr_porc_anual,prod_petroleo\
               FROM (SELECT * FROM prod_mundial_aux\
            WHERE prod_mundial_aux.id_prod NOT IN (SELECT id_prod FROM prod_mundial)) as tabla;")
        cursor.execute("DROP TABLE if exists prod_mundial_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        produccion_paises.to_sql("prod_pais_aux", con=engine, if_exists='replace',index=False)
        cursor.execute("CREATE TABLE IF NOT EXISTS prod_pais(id_prod text PRIMARY KEY,id_pais text, prod_carb_porc_anual float, prod_carbon float, prod_gas_porc_anual float,prod_gas float,prod_petr_porc_anual float, prod_petroleo float);")
        cursor.execute("INSERT INTO prod_pais (id_prod, id_pais, prod_carb_porc_anual,prod_carbon,prod_gas_porc_anual,prod_gas,prod_petr_porc_anual,prod_petroleo)\
            SELECT  id_prod, id_pais, prod_carb_porc_anual,prod_carbon,prod_gas_porc_anual,prod_gas,prod_petr_porc_anual,prod_petroleo  \
               FROM (SELECT * FROM prod_pais_aux\
            WHERE prod_pais_aux.id_prod NOT IN (SELECT id_prod FROM prod_pais)) as tabla;")
        cursor.execute("DROP TABLE if exists prod_pais_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        consumos_mundo.to_sql("cons_mundial_aux", con=engine, if_exists='replace',index=False)
        cursor.execute("CREATE TABLE IF NOT EXISTS cons_mundial(id_cons text PRIMARY KEY, id_pais text, con_bio_porc_anual float,con_biocomb float,con_carb_porc_anual float,con_carbon float, en_con_carb_porc_anual float, con_gas_porc_anual float,con_gas float,con_hydro_porc_anual float,con_hydro float,con_nuc_porc_anual float, con_nuc float, con_petr_porc_anual float, con_petr float, con_sol_porc_anual float, con_sol float, con_eol_porc_anual float,con_eol float);")
        cursor.execute("INSERT INTO cons_mundial (id_cons, id_pais, con_bio_porc_anual,con_biocomb,con_carb_porc_anual,con_carbon, en_con_carb_porc_anual, con_gas_porc_anual,con_gas,con_hydro_porc_anual,con_hydro,con_nuc_porc_anual, con_nuc, con_petr_porc_anual, con_petr, con_sol_porc_anual, con_sol, con_eol_porc_anual,con_eol)\
            SELECT  id_cons, id_pais, con_bio_porc_anual,con_biocomb,con_carb_porc_anual,con_carbon, en_con_carb_porc_anual, con_gas_porc_anual,con_gas,con_hydro_porc_anual,con_hydro,con_nuc_porc_anual, con_nuc, con_petr_porc_anual, con_petr, con_sol_porc_anual, con_sol, con_eol_porc_anual,con_eol \
               FROM (SELECT * FROM cons_mundial_aux\
            WHERE cons_mundial_aux.id_cons NOT IN (SELECT id_cons FROM cons_mundial)) as tabla;")
        cursor.execute("DROP TABLE if exists cons_mundial_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        consumos_paises.to_sql("con_pais_aux", con=engine, if_exists='replace',index=False)
        cursor.execute("CREATE TABLE IF NOT EXISTS cons_pais(id_cons text PRIMARY KEY, id_pais text, con_bio_porc_anual float,con_biocomb float,con_carb_porc_anual float,con_carbon float, en_con_carb_porc_anual float, con_gas_porc_anual float,con_gas float,con_hydro_porc_anual float,con_hydro float,con_nuc_porc_anual float, con_nuc float, con_petr_porc_anual float, con_petr float, con_sol_porc_anual float, con_sol float, con_eol_porc_anual float,con_eol float);")
        cursor.execute("INSERT INTO cons_pais (id_cons, id_pais, con_bio_porc_anual,con_biocomb,con_carb_porc_anual,con_carbon, en_con_carb_porc_anual, con_gas_porc_anual,con_gas,con_hydro_porc_anual,con_hydro,con_nuc_porc_anual, con_nuc, con_petr_porc_anual, con_petr, con_sol_porc_anual, con_sol, con_eol_porc_anual,con_eol)\
            SELECT  id_cons, id_pais, con_bio_porc_anual,con_biocomb,con_carb_porc_anual,con_carbon, en_con_carb_porc_anual, con_gas_porc_anual,con_gas,con_hydro_porc_anual,con_hydro,con_nuc_porc_anual, con_nuc, con_petr_porc_anual, con_petr, con_sol_porc_anual, con_sol, con_eol_porc_anual,con_eol \
               FROM (SELECT * FROM con_pais_aux\
            WHERE con_pais_aux.id_cons NOT IN (SELECT id_cons FROM cons_pais)) as tabla;")
        cursor.execute("DROP TABLE if exists con_pais_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        generacion_mundo.to_sql("gen_mundial_aux", con=engine, if_exists='replace',index=False)
        cursor.execute("CREATE TABLE IF NOT EXISTS gen_mundial(id_gen text PRIMARY KEY, id_pais text, poblacion float, pbi float, gen_biocomb float,gen_bio_porc float,gen_carbon float,gen_carbon_porc float,gen_gas float,gen_gas_porc float,gen_hidro float,gen_hidro_porc float, import_netas float,import_netas_porc float,gen_nuclear float,gen_nuclear_porc float,gen_petroleo float,gen_petroleo_porc float,gen_solar float,gen_solar_porc float,gen_eolica float,gen_eolica_porc float);")
        cursor.execute("INSERT INTO gen_mundial (id_gen, id_pais, poblacion, pbi, gen_biocomb,gen_bio_porc,gen_carbon,gen_carbon_porc,gen_gas,gen_gas_porc,gen_hidro,gen_hidro_porc, import_netas,import_netas_porc,gen_nuclear,gen_nuclear_porc,gen_petroleo,gen_petroleo_porc,gen_solar,gen_solar_porc,gen_eolica,gen_eolica_porc)\
            SELECT  id_gen, id_pais, poblacion, pbi, gen_biocomb,gen_bio_porc,gen_carbon,gen_carbon_porc,gen_gas,gen_gas_porc,gen_hidro,gen_hidro_porc, import_netas,import_netas_porc,gen_nuclear,gen_nuclear_porc,gen_petroleo,gen_petroleo_porc,gen_solar,gen_solar_porc,gen_eolica,gen_eolica_porc\
               FROM (SELECT * FROM gen_mundial_aux\
            WHERE gen_mundial_aux.id_gen NOT IN (SELECT id_gen FROM gen_mundial)) as tabla;")
        cursor.execute("DROP TABLE if exists gen_mundial_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        generacion_paises.to_sql("gen_pais_aux", con=engine, if_exists='replace',index=False)
        cursor.execute("CREATE TABLE IF NOT EXISTS gen_pais(id_gen text PRIMARY KEY, id_pais text, poblacion float, pbi float, gen_biocomb float,gen_bio_porc float,gen_carbon float,gen_carbon_porc float,gen_gas float,gen_gas_porc float,gen_hidro float,gen_hidro_porc float, import_netas float,import_netas_porc float,gen_nuclear float,gen_nuclear_porc float,gen_petroleo float,gen_petroleo_porc float,gen_solar float,gen_solar_porc float,gen_eolica float,gen_eolica_porc float);")
        cursor.execute("INSERT INTO gen_pais (id_gen, id_pais, poblacion, pbi, gen_biocomb,gen_bio_porc,gen_carbon,gen_carbon_porc,gen_gas,gen_gas_porc,gen_hidro,gen_hidro_porc, import_netas,import_netas_porc,gen_nuclear,gen_nuclear_porc,gen_petroleo,gen_petroleo_porc,gen_solar,gen_solar_porc,gen_eolica,gen_eolica_porc)\
            SELECT  id_gen, id_pais, poblacion, pbi, gen_biocomb,gen_bio_porc,gen_carbon,gen_carbon_porc,gen_gas,gen_gas_porc,gen_hidro,gen_hidro_porc, import_netas,import_netas_porc,gen_nuclear,gen_nuclear_porc,gen_petroleo,gen_petroleo_porc,gen_solar,gen_solar_porc,gen_eolica,gen_eolica_porc\
               FROM (SELECT * FROM gen_pais_aux\
            WHERE gen_pais_aux.id_gen NOT IN (SELECT id_gen FROM gen_pais)) as tabla;")
        cursor.execute("DROP TABLE if exists gen_pais_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        emision_co2_paises.to_sql("emision_pais_aux", con=engine, if_exists='replace',index=False)
        cursor.execute("DROP TABLE if exists emision_pais;")
        cursor.execute("CREATE TABLE IF NOT EXISTS emision_pais(id_emision text PRIMARY KEY, id_pais text, emision_co2 float, intensidad_carbono float);")
        cursor.execute("INSERT INTO emision_pais (id_emision, id_pais, emision_co2, intensidad_carbono)\
            SELECT  id_emision, id_pais, emision_co2, intensidad_carbono\
               FROM (SELECT * FROM emision_pais_aux\
            WHERE emision_pais_aux.id_emision NOT IN (SELECT id_emision FROM emision_pais)) as tabla;")
        cursor.execute("DROP TABLE if exists emision_pais_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        emision_mundial.to_sql("emision_mundial_aux", con=engine, if_exists='replace',index=False)
        cursor.execute("CREATE TABLE IF NOT EXISTS emision_mundial(id_emision text PRIMARY KEY, anio int, id_pais text, intensidad_carbono float, emision_co2 float, nivel_mar float, perdida_acumulada_glaciares_porc float, variacion_temperatura float);")
        cursor.execute("INSERT INTO emision_mundial (id_emision, anio, id_pais, intensidad_carbono, emision_co2, nivel_mar, perdida_acumulada_glaciares_porc, variacion_temperatura)\
            SELECT  id_emision, anio, id_pais, intensidad_carbono, emision_co2, nivel_mar, perdida_acumulada_glaciares_porc, variacion_temperatura\
               FROM (SELECT * FROM emision_mundial_aux\
            WHERE emision_mundial_aux.id_emision NOT IN (SELECT id_emision FROM emision_mundial)) as tabla;")
        cursor.execute("DROP TABLE if exists emision_mundial_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        tipo_planta.to_sql("tipo_planta_aux", con=engine, if_exists='replace',index=False)
        cursor.execute("CREATE TABLE IF NOT EXISTS tipo_planta(id_tipo_planta text PRIMARY KEY, tipo_planta text);")
        cursor.execute("INSERT INTO tipo_planta (id_tipo_planta, tipo_planta)\
            SELECT  id_tipo_planta, tipo_planta\
               FROM (SELECT * FROM tipo_planta_aux\
            WHERE tipo_planta_aux.id_tipo_planta NOT IN (SELECT id_tipo_planta FROM tipo_planta)) as tabla;")
        cursor.execute("DROP TABLE if exists tipo_planta_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        planta_energia.to_sql("planta_energia_aux", con=engine, if_exists='replace',index=False)
        cursor.execute("CREATE TABLE IF NOT EXISTS planta_energia(id_planta int PRIMARY KEY, id_pais text,id_tipo_planta text,nombre text,combustible_primario text,capacidad_mw float,latitud float,longitud float);")
        cursor.execute("INSERT INTO planta_energia (id_planta,id_pais,id_tipo_planta,nombre,combustible_primario,capacidad_mw,latitud,longitud)\
            SELECT  id_planta,id_pais,id_tipo_planta,nombre,combustible_primario,capacidad_mw,latitud,longitud\
               FROM (SELECT * FROM planta_energia_aux\
            WHERE planta_energia_aux.id_planta NOT IN (SELECT id_planta FROM planta_energia)) as tabla;")        
        cursor.execute("DROP TABLE if exists planta_energia_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        engine = sqlalchemy.create_engine('postgresql://pf10@pf10sql:!123henryfinal@pf10sql.postgres.database.azure.com/greendata')
        calidad_aire.to_sql("calidad_aire_aux", con=engine, if_exists='replace',index=False)          
        cursor.execute("CREATE TABLE IF NOT EXISTS calidad_aire(id_cal_aire text PRIMARY KEY, id_pais text, anio int, pm2_5_gm3 float,pm10_gm3 float,no2_gm3 float);")
        cursor.execute("INSERT INTO calidad_aire (id_cal_aire, id_pais, anio, pm2_5_gm3,pm10_gm3,no2_gm3)\
            SELECT  id_cal_aire, id_pais, anio, pm2_5_gm3,pm10_gm3,no2_gm3\
               FROM (SELECT * FROM calidad_aire_aux\
            WHERE calidad_aire_aux.id_cal_aire NOT IN (SELECT id_cal_aire FROM calidad_aire)) as tabla;")
        cursor.execute("DROP TABLE if exists calidad_aire_aux;")
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

try:
        conn_string = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE loc_pais ADD CONSTRAINT fk_pais_region FOREIGN KEY (id_region) REFERENCES loc_region;")
        cursor.execute("ALTER TABLE aniopais ADD CONSTRAINT fk_anio_pais FOREIGN KEY (id_pais) REFERENCES loc_pais;")
        cursor.execute("ALTER TABLE prod_pais ADD CONSTRAINT fk_prod_pais FOREIGN KEY (id_pais) REFERENCES loc_pais;")
        cursor.execute("ALTER TABLE prod_mundial ADD CONSTRAINT fk_prod_pais FOREIGN KEY (id_pais) REFERENCES loc_pais;")
        cursor.execute("ALTER TABLE gen_pais ADD CONSTRAINT fk_gen_pais FOREIGN KEY (id_pais) REFERENCES loc_pais;")
        cursor.execute("ALTER TABLE gen_mundial ADD CONSTRAINT fk_gen_pais FOREIGN KEY (id_pais) REFERENCES loc_pais;")
        cursor.execute("ALTER TABLE emision_pais ADD CONSTRAINT fk_emision_pais FOREIGN KEY (id_pais) REFERENCES loc_pais;")
        cursor.execute("ALTER TABLE emision_mundial ADD CONSTRAINT fk_emision_mundial FOREIGN KEY (id_pais) REFERENCES loc_pais;")
        cursor.execute("ALTER TABLE cons_pais ADD CONSTRAINT fk_cons_pais FOREIGN KEY (id_pais) REFERENCES loc_pais;")
        cursor.execute("ALTER TABLE cons_mundial ADD CONSTRAINT fk_cons_pais FOREIGN KEY (id_pais) REFERENCES loc_pais;")
        cursor.execute("ALTER TABLE planta_energia ADD CONSTRAINT fk_planta_tipo FOREIGN KEY (id_tipo_planta) REFERENCES tipo_planta;")
        cursor.execute("ALTER TABLE planta_energia ADD CONSTRAINT fk_planta_pais FOREIGN KEY (id_pais) REFERENCES loc_pais;")
        cursor.execute("ALTER TABLE calidad_aire ADD CONSTRAINT fk_calidad_pais FOREIGN KEY (id_pais) REFERENCES loc_pais;" )
        conn.commit()
        conn.close()
except (Exception, psycopg2.Error) as error:
        print(error)

print("Carga finalizada correctamente")
