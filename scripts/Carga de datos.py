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
loc_region.loc[loc_region['id_region']==5,'region']='Americas'
loc_pais.loc[loc_pais['pais']=='British Virgin Islands','region']='Americas'
loc_pais.loc[loc_pais['pais']=='Cape Verde','region']='Africa'
loc_pais.loc[loc_pais['pais']=="Cote d'Ivoire",'region']='Africa'
loc_pais.loc[loc_pais['pais']=='Brunei','region']='Asia'
loc_pais.loc[loc_pais['pais']=="Democratic Republic of Congo",'region']='Africa'
loc_pais.loc[loc_pais['pais']=="Faeroe Islands",'region']='Europe'
loc_pais.loc[loc_pais['pais']=="Falkland Islands",'region']='Americas'
loc_pais.loc[loc_pais['pais']=="Iran",'region']='Asia'
loc_pais.loc[loc_pais['pais']=="Laos",'region']='Asia'
loc_pais.loc[loc_pais['pais']=="Micronesia (country)",'region']='Oceania'
loc_pais.loc[loc_pais['pais']=="Moldova",'region']='Europe'
loc_pais.loc[loc_pais['pais']=="Netherlands Antilles",'region']='Americas'
loc_pais.loc[loc_pais['pais']=="North Korea",'region']='Asia'
loc_pais.loc[loc_pais['pais']=="Palestine",'region']='Asia'
loc_pais.loc[loc_pais['pais']=="Reunion",'region']='Africa'
loc_pais.loc[loc_pais['pais']=="Russia",'region']='Asia'
loc_pais.loc[loc_pais['pais']=="Saint Helena",'region']='Africa'
loc_pais.loc[loc_pais['pais']=="South Korea",'region']='Asia'
loc_pais.loc[loc_pais['pais']=="South Sudan",'region']='Africa'
loc_pais.loc[loc_pais['pais']=="Syria",'region']='Asia'
loc_pais.loc[loc_pais['pais']=="Taiwan",'region']='Asia'
loc_pais.loc[loc_pais['pais']=="Timor",'region']='Asia'
loc_pais.loc[loc_pais['pais']=="Turks and Caicos Islands",'region']='Americas'
loc_pais.loc[loc_pais['pais']=="Tuvalu",'region']='Oceania'
loc_pais.loc[loc_pais['pais']=="United Kingdom",'region']='Europe'
loc_pais.loc[loc_pais['pais']=="United States",'region']='Americas'
loc_pais.loc[loc_pais['pais']=="United States Virgin Islands",'region']='Americas'
loc_pais.loc[loc_pais['pais']=="Vietnam",'region']='Asia'
loc_pais.loc[loc_pais['region']=='Asia','id_region']=0
loc_pais.loc[loc_pais['region']=='Europe','id_region']=1
loc_pais.loc[loc_pais['region']=='Africa','id_region']=2
loc_pais.loc[loc_pais['region']=='Oceania','id_region']=3
loc_pais.loc[loc_pais['region']=='Americas','id_region']=5
loc_pais.sort_values('pais',inplace=True)

#Tabla hechos
tabla_hechos=pd.DataFrame(pd.read_csv(archivo5))
tabla_hechos=tabla_hechos[[ "country","year","iso_code","gdp", "coal_prod_change_pct","coal_production", "gas_prod_change_pct", "gas_production", "oil_prod_change_pct", "oil_production","biofuel_cons_change_pct", "biofuel_consumption", "coal_cons_change_pct", "coal_consumption","gas_cons_change_pct","gas_consumption", "hydro_cons_change_pct", "hydro_consumption", "nuclear_cons_change_pct", "nuclear_consumption", "oil_cons_change_pct", "oil_consumption", "solar_cons_change_pct", "solar_consumption", "wind_cons_change_pct", "wind_consumption","biofuel_electricity","biofuel_share_elec","coal_electricity","coal_share_elec","gas_electricity","gas_share_elec","hydro_electricity","hydro_share_elec","net_elec_imports","net_elec_imports_share_demand","nuclear_electricity","nuclear_share_elec","oil_electricity","oil_share_elec","solar_electricity","solar_share_elec","wind_electricity","wind_share_elec",'carbon_intensity_elec']]
tabla_hechos.rename(columns={'country':'pais','year':'anio','iso_code':'id_pais','gdp':'pbi','coal_prod_change_pct':"prod_carb_porc_anual",'coal_production':'prod_carbon','gas_prod_change_pct':'prod_gas_porc_anual','gas_production':'prod_gas','oil_prod_change_pct':'prod_petr_porc_anual','oil_production':'prod_petroleo','biofuel_cons_change_pct':'con_bio_porc_anual','biofuel_consumption':'con_biocomb','coal_cons_change_pct':'con_carb_porc_anual','coal_consumption':'con_carbon','gas_cons_change_pct':'con_gas_porc_anual','gas_consumption':'con_gas','hydro_cons_change_pct':'con_hydro_porc_anual','hydro_consumption':'con_hydro','nuclear_cons_change_pct':'con_nuc_porc_anual','nuclear_consumption':'con_nuc','oil_cons_change_pct':'con_petr_porc_anual','oil_consumption':'con_petr','solar_cons_change_pct':'con_sol_porc_anual','solar_consumption':'con_sol','wind_cons_change_pct':'con_eol_porc_anual','wind_consumption':'con_eol','biofuel_electricity':'gen_biocomb','biofuel_share_elec':'gen_bio_porc','coal_electricity':'gen_carbon','coal_share_elec':'gen_carbon_porc','gas_electricity':'gen_gas','gas_share_elec':'gen_gas_porc','hydro_electricity':'gen_hidro',"hydro_share_elec":"gen_hidro_porc","net_elec_imports":"import_netas","net_elec_imports_share_demand":"import_netas_porc","nuclear_electricity":"gen_nuclear","nuclear_share_elec":"gen_nuclear_porc","oil_electricity":"gen_petroleo","oil_share_elec":"gen_petroleo_porc","solar_electricity":"gen_solar","solar_share_elec":"gen_solar_porc","wind_electricity":"gen_eolica","wind_share_elec":"gen_eolica_porc",'carbon_intensity_elec':'intensidad_carbono'},inplace=True)
tabla_hechos=tabla_hechos[tabla_hechos['anio']>=1980]
tabla_hechos=tabla_hechos.merge(loc_pais, how="right", on=["pais"])
tabla_hechos.drop(columns={'id_pais_x','region'},inplace=True)
tabla_hechos.rename(columns={'id_pais_y':'id_pais'},inplace=True)
tabla_hechos=tabla_hechos.reindex(columns=['id_pais','pais','anio','id_region','pbi','prod_carb_porc_anual','prod_carbon',"prod_carb_porc_anual",'prod_carbon','prod_gas_porc_anual','prod_gas','prod_petr_porc_anual','prod_petroleo','con_bio_porc_anual','con_biocomb','con_carb_porc_anual','con_carbon','con_gas_porc_anual','con_gas','con_hydro_porc_anual','con_hydro','con_nuc_porc_anual','con_nuc','con_petr_porc_anual','con_petr','con_sol_porc_anual','con_sol','con_eol_porc_anual','con_eol','gen_biocomb','gen_bio_porc','gen_carbon','gen_carbon_porc','gen_gas','gen_gas_porc','gen_hidro',"gen_hidro_porc","import_netas","import_netas_porc","gen_nuclear","gen_nuclear_porc","gen_petroleo","gen_petroleo_porc","gen_solar","gen_solar_porc","gen_eolica","gen_eolica_porc",'intensidad_carbono'])
tabla_hechos.insert(0,'id',tabla_hechos['id_pais']+'-'+tabla_hechos['anio'].map(str))

# Tabla emisiones de CO2
emision_co2 = pd.DataFrame(pd.read_csv(archivo1, delimiter=";"))
emision_co2=emision_co2[['Year','Country','CO2_emission']]
emision_co2=emision_co2.groupby(['Year','Country'], as_index=False).sum()
emision_co2.rename(columns={'Year':'anio','Country':'pais'},inplace=True)
tabla_hechos=tabla_hechos.merge(emision_co2,how='left',on=['anio','pais'])

# Variación masa glaciares
glaciares=pd.DataFrame(pd.read_csv(archivo2, delimiter=";"))
glaciares=glaciares[glaciares['Year']>=1980]
glaciares.rename(columns={'Year':'anio','Mean cumulative mass balance':'perdida_acum_glaciares_porc'},inplace=True)
glaciares.drop(columns=['Number of observations'], inplace=True)
glaciares.insert(0,'pais','World')
tabla_hechos=tabla_hechos.merge(glaciares,how='left',on=['anio','pais'])

# Variación nivel del mar
nivel=pd.DataFrame(pd.read_csv(archivo4, delimiter=";"))
nivel=nivel[nivel['Year']>=1980]
nivel.reset_index(inplace=True,drop=True)
nivel['NOAA Adjusted Sea Level']=nivel['NOAA Adjusted Sea Level'].fillna(nivel['CSIRO Adjusted Sea Level'])
nivel['CSIRO Adjusted Sea Level']=nivel['CSIRO Adjusted Sea Level'].fillna(nivel['NOAA Adjusted Sea Level'])
nivel['Mean_level']=(nivel['CSIRO Adjusted Sea Level']+nivel['NOAA Adjusted Sea Level'])/2
nivel=nivel[['Year','Mean_level']]
nivel.rename(columns={'Year':'anio','Mean_level':'nivel_mar'},inplace=True)
nivel.insert(0,'pais','World')
tabla_hechos=tabla_hechos.merge(nivel,how='left',on=['anio','pais'])

# Variación de temperatura
var_temp=pd.DataFrame(pd.read_csv(archivo6, delimiter=";"))
var_temp=var_temp[var_temp['Year']>=1980]
var_temp=var_temp.groupby('Year', as_index=False).mean()
var_temp.rename(columns={'Year':'anio','Mean':'variacion_temp'},inplace=True)
var_temp.insert(0,'pais','World')
tabla_hechos=tabla_hechos.merge(var_temp,how='left',on=['pais','anio'])

# Tabla tipo de planta
cod_planta={'id_tipo_planta':['LC','Fos','Alm','Cog','Otr'],'tipo_planta':['Low_Carbon','Fosil','Almacenamiento','Cogeneracion','Otros']}
tipo_planta=pd.DataFrame(cod_planta)

# Tabla calidad del aire
calidad_aire=pd.DataFrame(pd.read_csv(archivo0,sep=';'))
calidad_aire=calidad_aire[['ISO3','WHO Country Name','City or Locality','Measurement Year','PM2.5 (?g/m3)','PM10 (?g/m3)','NO2 (?g/m3)']]
calidad_aire=calidad_aire.groupby(['WHO Country Name','Measurement Year'],as_index=False).sum()
calidad_aire.rename({'WHO Country Name':'pais','Measurement Year':'anio','PM2.5 (?g/m3)':'pm2_5_gm3','PM10 (?g/m3)':'pm10_gm3','NO2 (?g/m3)':'no2_gm3'},axis=1,inplace=True)
calidad_aire[calidad_aire['pais']=='United Republic of Tanzania'] = calidad_aire[calidad_aire['pais']=='United Republic of Tanzania'].fillna("TZA")
calidad_aire[calidad_aire['pais']=='United States of America'] = calidad_aire[calidad_aire['pais']=='United States of America'].fillna("USA")
calidad_aire[calidad_aire['pais']=='Venezuela (Bolivarian Republic of)'] = calidad_aire[calidad_aire['pais']=='Venezuela (Bolivarian Republic of)'].fillna("VEN")
calidad_aire[calidad_aire['pais']=='Viet Nam'] = calidad_aire[calidad_aire['pais']=='Viet Nam'].fillna("VNM")
tabla_hechos=tabla_hechos.merge(calidad_aire,how='left',on=['pais','anio'])
tabla_hechos.drop(columns={'pais'},inplace=True)


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

# Separar tablas por localización
tabla_hechos_mundo=tabla_hechos[tabla_hechos['id_pais']=='WRL']
tabla_hechos_pais=tabla_hechos[tabla_hechos['id_pais']!='WRL']
tabla_hechos_pais.reset_index(inplace=True,drop=True)
tabla_hechos_pais.drop(columns={'perdida_acum_glaciares_porc','nivel_mar','variacion_temp'},inplace=True)
tabla_hechos_mundo.reset_index(inplace=True,drop=True)
loc_pais=loc_pais.loc[loc_pais['pais']!='World',:]
loc_region=loc_region.loc[loc_region['region']!='World']

loc_region.to_csv(r"C:\Users\juli_\Desktop\Data diego\loc_region.csv")
loc_pais.to_csv(r"C:\Users\juli_\Desktop\Data diego\loc_pais.csv")
planta_energia.to_csv(r"C:\Users\juli_\Desktop\Data diego\planta_energia.csv")
tipo_planta.to_csv(r"C:\Users\juli_\Desktop\Data diego\tipo_planta.csv")
tabla_hechos_mundo.to_csv(r"C:\Users\juli_\Desktop\Data diego\hechos_mundo.csv")
tabla_hechos_pais.to_csv(r"C:\Users\juli_\Desktop\Data diego\hechos_pais.csv")

