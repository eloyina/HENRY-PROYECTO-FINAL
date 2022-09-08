import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.statespace.sarimax import SARIMAX
hechos = pd.read_csv(r"C:\Users\juli_\Downloads\hechos_mundial.csv", delimiter=";")

#Predicción glaciares
glaciers = hechos[["anio","perdida_acum_glaciares_porc"]]
glaciers["perdida_acum_glaciares_porc"] = glaciers["perdida_acum_glaciares_porc"]*0.001
glaciers["anio"] = pd.to_datetime(glaciers["anio"],format="%Y")
glaciers.set_index(["anio"], inplace=True)
#Proceso de ML
model = SARIMAX(glaciers, order=(1, 0, 0), seasonal_order=(1, 1, 0, 7), trend='c')
results = model.fit()
forecast_object = results.get_forecast(9)
mean = forecast_object.predicted_mean   
dates = mean.index
#Gráfico
plt.figure()
# Datos
plt.plot(glaciers.index, glaciers, color = "lightblue", label='Datos')
# Prediccion    
plt.plot(dates, mean, linestyle = "--",  color = "red", label='Predicción')
plt.ylabel("Masa glacial - %")
plt.legend()
plt.title("Predicción cambio en la masa glacial")
plt.show()

#Predicción nivel del mar
nivel_mar = hechos[["anio","nivel_mar"]]
nivel_mar["anio"] = pd.to_datetime(nivel_mar["anio"],format="%Y")
nivel_mar.set_index(["anio"], inplace=True)
#Proceso de ML
model = SARIMAX(nivel_mar, order=(1, 0, 0), seasonal_order=(1, 1, 0, 7), trend='c')
results = model.fit()
forecast_object = results.get_forecast(12)
mean = forecast_object.predicted_mean   
dates = mean.index
#Gráfico
plt.figure()
# Datos
plt.plot(nivel_mar.index, nivel_mar, color = "lightblue", label='Datos')
# Prediccion    
plt.plot(dates, mean, linestyle = "--",  color = "red", label='Predicción')
plt.ylabel("Nivel del mar - mm")
plt.legend()
plt.title("Predicción cambio en el nivel del mar")
plt.show()

#Predicción temperatura
var_temp = hechos[["anio","variacion_temp"]]
var_temp["anio"] = pd.to_datetime(var_temp["anio"],format="%Y")
var_temp.set_index(["anio"], inplace=True)
#Proceso de ML
model = SARIMAX(var_temp, order=(1, 0, 0), seasonal_order=(1, 1, 0, 7), trend='c')
results = model.fit()
forecast_object = results.get_forecast(10)
mean = forecast_object.predicted_mean
dates = mean.index
#Gráfico
plt.figure()
# Datos
plt.plot(var_temp.index, var_temp, color = "blue", label='Datos')
# Prediccion    
plt.plot(dates, mean, linestyle = "--",  color = "red", label='Predicción')
plt.ylabel("Temperatura - °C")
plt.legend()
plt.title("Predicción cambio de temperatura")
plt.show()