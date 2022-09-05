import streamlit as st
import matplotlib.pyplot as plt
import numpy as np
import time
import pandas as pd
import numpy as np
import plotly.express as px
import datetime
# Import required libraries
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')
from statsmodels.graphics import tsaplots
from statsmodels.tsa.seasonal import seasonal_decompose

from statsmodels.tsa.statespace.sarimax import SARIMAX
"""
df = pd.read_csv("CO2.csv",parse_dates=True)
print(df.head())
df['day']='01'
cols=["year","month","day"]
df['date'] = df[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns")
print(df['date'].head)
df2= df[['date','deseasonalized']]
df2.to_csv('out.csv')
"""



co2 = pd.read_csv('out.csv', index_col='date', parse_dates=True)
co2.pop('id')
print(co2.head())


print(co2.head())
co2 = co2.asfreq('MS')
print(co2.head())
model = SARIMAX(co2, order=(1, 1, 1), seasonal_order=(0, 1, 1, 12), trend='c')


ax3 = co2.plot(kind='density', linewidth=2)
ax3.set_xlabel('Your first density plot')
ax3.set_ylabel('Density values of your data')
ax3.set_title('Density plot of your data')
plt.show()

index_month = co2.index.date
co2= co2.groupby(index_month).mean()
co2.plot()

plt.show()

# Fit model
results = model.fit()
print(results.summary())
results.plot_diagnostics(figsize=(20, 15));
plt.tight_layout();
plt.show()

forecast_object = results.get_forecast(136)

# Extract predicted mean attribute
mean = forecast_object.predicted_mean

# Calculate the confidence intervals
conf_int = forecast_object.conf_int()

print(conf_int['lower mean'])
# Extract the forecast dates
dates = mean.index

plt.figure()

# Plot past CO2 levels
plt.plot(co2.index, co2, label='pasado');

# Plot the prediction means as line
plt.plot(dates, mean, label='predicción');



# Plot legend
plt.legend();
plt.show()

print(mean.iloc[-1])

# Print last confidence interval
print(conf_int.iloc[-1])

