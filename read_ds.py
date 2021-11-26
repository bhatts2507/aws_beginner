from numpy import int64
import pandas as pd

#reading csv
df=pd.read_csv("D:/out_20211126121433.csv")
value = input("Please number of rows of blocks of dataframe:")
number_of_rows=int(value)   # converting to integer type
my_df=pd.DataFrame()        #creating blank dataframe  
number_of_blocks = df.shape[0]/int(value)  #calculating the no of iteration for the loop
b=int(number_of_blocks)     # converting to integer type
first_row=0                 #initialization
last_row=number_of_rows
for i in range(b):
    df1=df.loc[first_row:last_row]    #slicing the original dataframe
    mean_atm = df1['_ATMOSPHERIC_PRESSURE__65577_0_36'].mean()
    mean_air_temp=df1['_AMBIENT_AIR_TEMPERATURE__66025_0_36'].mean()
    new_row = {'EVENT_TS_MILLIS':df1.loc[first_row, 'EVENT_TS_MILLIS'], 'EVENT_TIMESTAMP':df1.loc[first_row, 'EVENT_TIMESTAMP'], 'SERIAL_NUMBER':df1.loc[first_row, 'SERIAL_NUMBER'], '_ATMOSPHERIC_PRESSURE__65577_0_36':mean_atm,'_AMBIENT_AIR_TEMPERATURE__66025_0_36':mean_air_temp}
    my_df = my_df.append(new_row, ignore_index=True)   #row added 
    first_row=first_row+number_of_rows
    last_row=last_row+number_of_rows
  

my_df = my_df[['EVENT_TS_MILLIS','EVENT_TIMESTAMP','SERIAL_NUMBER','_AMBIENT_AIR_TEMPERATURE__66025_0_36','_ATMOSPHERIC_PRESSURE__65577_0_36']]
my_df['EVENT_TS_MILLIS'] = my_df['EVENT_TS_MILLIS'].astype(int64)
#my_df.to_csv("D:/downsized_data.csv")
print(my_df)