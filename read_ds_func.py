from numpy import int64
import os
import pandas as pd
value= int(os.environ['sample_size'])
input_file= "s3://bhatts-core/out_20211126121433.csv"
output_file = "s3://new-bucket-pp/out_20211126121433.csv"
#reading csv
def csv_read():
    df=pd.read_csv(input_file)
    return df

def create_blank_df():
    my_df=pd.DataFrame() 
    return my_df

def user_row_value():
    #value = input("Please number of rows of blocks of dataframe:")
    value= os.environ['sample_size']
    number_of_rows=int(value)   # converting to integer type
    return number_of_rows

def calculate_block(read_df,row_num):
    number_of_blocks = read_df.shape[0]/int(row_num)  #calculating the no of iteration for the loop
    b=int(number_of_blocks)     # converting to integer type
    return b

def iterate(block,row_num, read_df, ndf):
    first_row=0                 #initialization
    last_row=row_num
    for i in range(block):
        df1=read_df.loc[first_row:last_row]    #slicing the original dataframe
        mean_atm = int(round(df1['_ATMOSPHERIC_PRESSURE__65577_0_36'].mean(),0))
        mean_air_temp=int(round(df1['_AMBIENT_AIR_TEMPERATURE__66025_0_36'].mean(),0))
        new_row = {'EVENT_TS_MILLIS':df1.loc[first_row, 'EVENT_TS_MILLIS'], 'EVENT_TIMESTAMP':df1.loc[first_row, 'EVENT_TIMESTAMP'], 'SERIAL_NUMBER':df1.loc[first_row, 'SERIAL_NUMBER'], '_ATMOSPHERIC_PRESSURE__65577_0_36':mean_atm,'_AMBIENT_AIR_TEMPERATURE__66025_0_36':mean_air_temp}
        ndf = ndf.append(new_row, ignore_index=True)   #row added 
        first_row=first_row+row_num
        last_row=last_row+row_num
    return ndf

def display_df(final_df):
    final_df = final_df[['EVENT_TS_MILLIS','EVENT_TIMESTAMP','SERIAL_NUMBER','_AMBIENT_AIR_TEMPERATURE__66025_0_36','_ATMOSPHERIC_PRESSURE__65577_0_36']]
    final_df['EVENT_TS_MILLIS'] = final_df['EVENT_TS_MILLIS'].astype(int64)
    final_df.to_csv(output_file)
    #print(final_df)



def lambda_handler(event, context):
    global value
    read_df = csv_read()
    ndf = create_blank_df()
    #row_num = user_row_value()
    block = calculate_block(read_df,value)
    final_df = iterate(block, value, read_df, ndf)
    display_df(final_df)
    response = {'Name': "Success"}
    return response

    



