import sys
import time
import datetime
import csv
import random
import pandas as pd

def main(argv,rows):

    print("------------Generating Data for {} rows every second------------".format(str(rows)))
    now = datetime.datetime.now()
    str_now = now.strftime('%Y%m%d%H%M%S')
    outcsv = "out_{}.csv".format(str_now)
    fieldnames = ['EVENT_TS_MILLIS','EVENT_TIMESTAMP','SERIAL_NUMBER','_AMBIENT_AIR_TEMPERATURE__66025_0_36','_ATMOSPHERIC_PRESSURE__65577_0_36','ROW_NUM']
    with open(outcsv, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(1,rows+1):
            serial_number='VT-906'
            time=now+datetime.timedelta(0,i)
            #end_time=time + datetime.timedelta(0,random.random()*100)
            str_time=time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            #str_end_time=end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            #str_date=time.strftime('%m/%d/%Y')
            militime=int(round((time.timestamp()*1000)))
        
            #Create Json Data
            data = {}
            data['_AMBIENT_AIR_TEMPERATURE__66025_0_36']=int(random.random()*100)
            data['_ATMOSPHERIC_PRESSURE__65577_0_36']=int(random.random()*500)
            data['EVENT_TS_MILLIS']=militime
            data['EVENT_TIMESTAMP']=str_time
            data['SERIAL_NUMBER']=serial_number
            data['ROW_NUM']=i
            writer.writerow(data)


if __name__ == "__main__":
    start_time = time.time()
    argv=sys.argv[1:]
    #print(len(argv))
    rows = input ("Enter number of rows to be generated (Default is 10 hours - 36000 rows) :")
    if(len(rows) == 0):
        rows = 36000
    main(argv,int(rows))
    exec_time = round((time.time() - start_time), 2)
    print("------------Processed in {} seconds------------".format(str(exec_time)))

