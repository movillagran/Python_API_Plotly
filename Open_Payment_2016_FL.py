import pandas as pd
import numpy as np
import requests
import json
from pandas import ExcelWriter
from pandas import ExcelFile
from sodapy import Socrata
from pandasql import sqldf, PandaSQL

#https://openpaymentsdata.cms.gov/dataset/General-Payment-Data-Detailed-Dataset-2016-Reporti/3cy7-uu8k

#https://dev.socrata.com/foundry/openpaymentsdata.cms.gov/qh6m-nw4f

#app token: Sl6J3DIYj30hXGdoVuLOEday5

#secret token: MpGu26d-7Xdq2nAefJ8KKXoWcka0pPcHv4tT

pysqldf = lambda q: sqldf(q, globals())

# Create an unauthenticated client for public datasets.
# client = Socrata("data.medicare.gov", None)

# Create an authenticated client for non-public datasets.
# client = Socrata("data.medicare.gov","Sl6J3DIYj30hXGdoVuLOEday5",username="mo.villagran@gmail.com",password="Mojasmine0805!")



# Web API's URL endpoint.
baseUrl = "https://openpaymentsdata.cms.gov/resource/qh6m-nw4f.json?$$app_token=Sl6J3DIYj30hXGdoVuLOEday5"

# Web API's URL endpoint for the query "SELECT COUNT(*) WHERE recipient_zip_code = "06510"", to count the total row 
url = baseUrl + "&$query=SELECT%20COUNT(*)%20WHERE%20recipient_state%20=%22FL%22"

o_url = "https://openpaymentsdata.cms.gov/resource/qh6m-nw4f.json?$query=SELECT%20recipient_state%2crecipient_zip_code%2ctotal_amount_of_payment_usdollars%20WHERE%20recipient_state%20=%22FL%22"
print(o_url)

# Send an HTTP GET request to the URL endpoint.
response = requests.get(url)

# Convert  the response to json format.
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
	# Web API's URL endpoint for recipient's living in the zip code 06510.
	#url = o_url + "&$limit=" + str(limit) + "&$offset=" + str(offset) + "&recipient_state=FL"
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

dataFrames.recipient_zip_code = dataFrames.recipient_zip_code.str[:5]

dataFrames.to_csv('C:\\Users\\mo.villagran\\Desktop\\Python_API_JSON\\FL_out.csv', sep=',')


dataFrames.info()



#Load CVS FL 2016 Open Payment data

FL_df = pd.read_csv('C:\\Users\\mo.villagran\\Desktop\\Python_API_JSON\\FL_out.csv')

FL_df.info()
FL_df['recipient_zip_code'] = FL_df.recipient_zip_code.astype(str)
FL_df.recipient_zip_code = FL_df.recipient_zip_code.str[:5]


#Load zip code file

zip_df = pd.read_excel('C:\\Users\\mo.villagran\\Desktop\\Python_API_JSON\\Zipcode_table.xlsx',sheet_name='Main')
zip_df['FIPS'] = zip_df.FIPS.astype(str)
#print("Column headings:")
#print(df.columns)



#join here

pdsql = PandaSQL()
zip_df['FIPS'] = zip_df.FIPS.astype(str)
joined_df = pdsql("Select a.recipient_zip_code, a.total_amount_of_payment_usdollars, b.State, b.County, b.FIPS from FL_df a left join zip_df b on a.recipient_zip_code = b.ZipCode and a.recipient_state = b.State" )

joined_df['total_amount_of_payment_usdollarsz'] = joined_df.total_amount_of_payment_usdollars.astype(float)

joined_df = joined_df.drop_duplicates(subset=None, keep="first", inplace=False)



df = joined_df.fillna(0)
df= joined_df.round(0)

df=df.rename(columns = {'recipient_zip_code':'Zip_Code'})
df=df.rename(columns = {'total_amount_of_payment_usdollars':'Total_Payment'})
df['Total_Payment'] = df.Total_Payment.astype(float)
df['Total_Payment'] = df.Total_Payment.round(0)

df = df.groupby(['FIPS','State','County'])['Total_Payment'].sum().astype(float)
df = df.reset_index()


trash = ['0','None', 0]
	
df_final =  df[~df.FIPS.isin(trash)]


print(df_final)

df_final.to_csv('C:\\Users\\mo.villagran\\Desktop\\Python_API_JSON\\Combo_FL_out.csv', sep=',')


#~~~~~~visualization

import plotly
plotly.tools.set_credentials_file(username='mo.villagran', api_key='4MpuZPkRTuRdfRLqcp3a')


import plotly.plotly as py
import plotly.figure_factory as ff

import numpy as np
import pandas as pd

df_sample = pd.read_csv('C:\\Users\\mo.villagran\\Desktop\\Python_API_JSON\\Combo_FL_out.csv')

#df_sample = df_sample[df_sample['State'] == 'FL']

Total_Payment = df_sample['Total_Payment'].tolist()
fips = df_sample['FIPS'].tolist()

endpts = [2000000,4000000,6000000,8000000,10000000,12000000,14000000,16000000]
#endpts = list(np.mgrid[min(values):max(values):4j])
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
plotly.offline.plot(fig, filename='US_Open_Payment_2016_FL.html')
