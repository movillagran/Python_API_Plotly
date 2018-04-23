

import pandas as pd
import numpy as np

import requests
import json

from pandas import ExcelWriter
from pandas import ExcelFile

from pandasql import sqldf, PandaSQL
pysqldf = lambda q: sqldf(q, globals())

import plotly
plotly.tools.set_credentials_file(username='Your User Name', api_key='Your Api Key')
import plotly.plotly as py
import plotly.figure_factory as ff

# Link to the 2016 Open Payment data:
# https://openpaymentsdata.cms.gov/dataset/General-Payment-Data-Detailed-Dataset-2016-Reporti/3cy7-uu8k

# 2016 Open Payment data API documentation:
# https://dev.socrata.com/foundry/openpaymentsdata.cms.gov/qh6m-nw4f

# Please create a CMS account at https://data.cms.gov/
# Obtain app_token here https://dev.socrata.com/docs/app-tokens.html

# Web API's URL endpoint.
baseUrl = "https://openpaymentsdata.cms.gov/resource/qh6m-nw4f.json?" +\
"$$app_token=Your App Token"

# Web API's URL endpoint for the query "SELECT COUNT(*) WHERE recipient_state = "FL"
# Count the total row 
url = baseUrl + "&$query=SELECT%20COUNT(*)%20WHERE%20recipient_state%20=%22FL%22"

# The actual columns from the JSON were selected in query parameters
o_url = "https://openpaymentsdata.cms.gov/resource/qh6m-nw4f.json?$" + \
"query=SELECT%20recipient_state%2crecipient_zip_code" +\
"%2ctotal_amount_of_payment_usdollars%20WHERE%20recipient_state%20=%22FL%22"

# Send an HTTP GET request to the URL endpoint.
response = requests.get(url)

# Convert the response to json format.
json = response.json()

# Extract the count variable from the json object, and cast it to an integer.
count = int(json[0]["COUNT"])

print("Count: " + str(count))


# Create a data frame.
dataFrames = pd.DataFrame()

# Set a limit off 2000 rows per request.
limit = 2000

# Iterate from 0 to count in limit increments.
for offset in range(0, count, limit):
	# Web API's URL endpoint for recipient's living in Florida.
	url = o_url + "%20limit%20" + str(limit) + "%20offset%20" + str(offset) 
	
	# Send an HTTP GET request to the URL endpoint.
	response = requests.get(url)
	
	# Convert  the response to json format.
	json = response.json()
	
	# Print the response to the console.

	print(json)
	
	# Create a data frame from the json object.
	dataFrame = pd.DataFrame(json)
	
	# Append the data frame to the big data frame.
	dataFrames = dataFrames.append(dataFrame, ignore_index=True)
	
	print("Offset: " + str(offset))

print("Number of rows in final data frame: " + str(len(dataFrames)))

dataFrames.recipient_zip_code = dataFrames.recipient_zip_code.str[:5]  #Get rid of the extra 4 digits in the end

dataFrames.to_csv('Your Directory\\FL_out.csv', sep=',')

dataFrames.info()


#Load CSV FL 2016 Open Payment data
FL_df = pd.read_csv('Your Directory\\FL_out.csv')
FL_df.info()
FL_df['recipient_zip_code'] = FL_df.recipient_zip_code.astype(str)
FL_df.recipient_zip_code = FL_df.recipient_zip_code.str[:5]  #Get rid of the extra 4 digits in the end

#Load zip code file
zip_df = pd.read_excel('Your Directory\\Zipcode_table.xlsx',sheet_name='Main') #zip code  table is in the folder here in Github.
zip_df['FIPS'] = zip_df.FIPS.astype(str)

#join CMS 2016 open payment FL with Zipcode_table to obtain FIPS here
pdsql = PandaSQL()
zip_df['FIPS'] = zip_df.FIPS.astype(str)
joined_df = pdsql("Select a.recipient_zip_code, a.total_amount_of_payment_usdollars, b.State, b.County, b.FIPS" +\ 
"from FL_df a left join zip_df b on a.recipient_zip_code = b.ZipCode and a.recipient_state = b.State" )
joined_df['total_amount_of_payment_usdollarsz'] = joined_df.total_amount_of_payment_usdollars.astype(float)

#Clean up data due to duplicates, nulls, etc.
joined_df = joined_df.drop_duplicates(subset=None, keep="first", inplace=False)
df = joined_df.fillna(0)
df= joined_df.round(0)

#Rename some columns for better presentation
df=df.rename(columns = {'recipient_zip_code':'Zip_Code'})
df=df.rename(columns = {'total_amount_of_payment_usdollars':'Total_Payment'})
df['Total_Payment'] = df.Total_Payment.astype(float)
df['Total_Payment'] = df.Total_Payment.round(0)

#Summarize total open payment by FIPS level because Plotly is at the county level
df = df.groupby(['FIPS','State','County'])['Total_Payment'].sum().astype(float)
df = df.reset_index()

#Clean up data again due to duplicates, nulls, etc.
trash = ['0','None', 0]
df_final =  df[~df.FIPS.isin(trash)]

df_final.to_csv('Your Directory\\Combo_FL_out.csv', sep=',')


#~~~~~~Makine a Choropleth Map using Plotly~~~~~~~~

# You will also need to register for an account on plotly here:
# https://plot.ly/ (It's free to get an account.)

# You can read in the dataframes from above without the program,
# but it's easier to modify Plotly graph when you have CSV files downloaded
# so you don't have to rerun the whole program from the top.

df_sample = pd.read_csv('Your Directory\\Combo_FL_out.csv')
Total_Payment = df_sample['Total_Payment'].tolist()
fips = df_sample['FIPS'].tolist()

# These end points need to be estimated according to the data. 
endpts = [2000000,4000000,6000000,8000000,10000000,12000000,14000000,16000000]

# I used this website here: http://www.perbang.dk/rgbgradient/
# To create my own gradient
colorscale = [
    "#99DEE5",
    "#00E096",
	"#00DB38",
	"#1400D6",
	"#5900D2",
	"#9C00CD",
	"#C800B4",
	"#C3006E",
	"#BF002C"
]

fig = ff.create_choropleth(
    fips=fips, values=Total_Payment, scope=['Florida'], show_state_data=True,
    colorscale=colorscale, binning_endpoints=endpts, round_legend_values=True,
    plot_bgcolor='rgb(229,229,229)',
    paper_bgcolor='rgb(229,229,229)',
    legend_title='Open Payment by County',
    county_outline={'color': 'rgb(255,255,255)', 'width': 0.5}
)

# You can do the online version but the offline graph is faster.
plotly.offline.plot(fig, filename='US_Open_Payment_FL_2016.html')


