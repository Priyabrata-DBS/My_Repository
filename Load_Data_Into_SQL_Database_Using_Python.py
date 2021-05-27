## importing all required libraries for
## cleansing data set, numeric operations,
## date time conversions and connecting DB server
import pandas as pd
import numpy as np
import datetime
import pyodbc
print("##########################################################")
print("pandas, numpy, datetime, pyodbc libraries imported successfully")
print("##########################################################")
#1 Loading the CSV data into Pandas Data Frame
def load_csv():
    try:
        df = pd.read_csv("FireBrigadeAndAmbulanceCallOuts.CSV")
        print("\n##########################################################")
        print("\n Dataset loaded successfully into Python Data Frame")
        print("\n##########################################################")
        return df
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while loading CSV file: ", ex)
df = load_csv()

#2 Total number of rows and columns
def no_of_row_column(df):
    try:
        print("\n##########################################################")
        print("\n Q.2 Total number of rows and columns")
        print("\n##########################################################")
        print ("\nNumber of rows: ", df.shape[0])
        print ("\nNumber of columns: ", df.shape[1])
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while displaying number of rows and columns: ", ex)
no_of_row_column(df)

#3 Output the number of non-null rows (by column).
def no_of_non_null_rows_by_column(df):
    try:
        print("\n##########################################################")
        print("\n Q.3 The number of non null rows per column are showing below: ")
        print("\n##########################################################")
        print()
        print(df.notnull().sum())
        print()
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while displaying number of non null rows by columns: ", ex)
no_of_non_null_rows_by_column(df)

#4 Output the number of null values (by column).
def no_of_null_rows_by_column(df):
    try:
        print("\n#######################################################")
        print("\n Q.4 The number of null rows per column are showing below: ")
        print("\n#######################################################")
        print()
        print(df.isnull().sum())
        print()
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while displaying number of null rows by columns: ", ex)
no_of_null_rows_by_column(df)


#5 Output the number of null values for all columns.
def total_no_of_null_rows(df):
    try:
        print("\n############################################################")
        print("\n Q.5 Total number of null row is : ",df.isnull().sum().sum())
        print("\n############################################################")
        print()
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while displaying total number of rows: ", ex)
total_no_of_null_rows(df)


#6 Output the total number of call outs by Station Area
def total_no_of_call_out_by_station(df):
    try:
        print("\n####################################################################")
        print("\n Q.6 The number of call outs by Station Area are shown below: ")
        print("\n####################################################################")
        print()
        print (df.groupby(['Station Area'],as_index=False) ['TOC'].count().rename(columns={'TOC': 'No_of_call_outs'}))
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while displaying number of call outs by Station Area: ", ex)
total_no_of_call_out_by_station(df)


#7 Output the total number of call outs by Date and Station Area
def total_no_of_call_out_by_date_station(df):
    try:
        print("\n####################################################################")
        print("\n Q.7 The number of call outs by Station Area and Date are shown below: ")
        print("\n####################################################################")
        print()
        print (df.groupby(['Station Area','Date'],as_index=False) ['TOC'].count().rename(columns={'TOC': 'No_of_call_outs'}))
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while displaying number of call outs by Station Area and Date: ", ex)
total_no_of_call_out_by_date_station(df)

#8 Output the total number of call outs by Station Area and Date where the description is either Fire Car
#or Fire Alarm
def call_out_by_date_station_fire(df):
    try:
        print("\n####################################################################")
        print("\n Q.8 The number of call outs by Station Area and Date where the description is Fire Car or Fire Alarm are shown below: ")
        print("\n####################################################################")
        print()
        filtered_df = df[(df['Description']=='Fire ALARM') | (df['Description']=='Fire CAR')]
        print (filtered_df.groupby(['Station Area','Date'],as_index=False) ['TOC'].count().rename(columns={'TOC': 'No_of_call_outs'}))
        print()
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while displaying number of call outs by Station Area and Date where the description is Fire Car or Fire Alarm: ", ex)
call_out_by_date_station_fire(df)

##9 Replace any instance of “,” (in any column) with an empty string.
def replace_comma(df):
    try:
        print("\n####################################################################")
        print("Q.9 Replace any instance of “,” (in any column) with an empty string.")
        print("######################################################################")
        df = df.replace(r',', '', regex=True)
        print("\nSuccessfully removed Comma(,) from every column")
        return df
    ####(df.AH == ',').sum() ## Testing of replacing comma
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while replacing comma: ", ex)
df = replace_comma(df)

###10  Replace any instance of “-” (in any column) with an empty string.
def replace_hyphen(df):
    try:
        print("\n####################################################################")
        print("Q.10 Replace any instance of “-” (in any column) with an empty string.")
        print("######################################################################")
        df.iloc[:,1:] = df.iloc[:,1:].replace('-','', regex=True)
        print("\nSuccessfully removed Hyphen(-) from every column except 'Date' column")
        return df
        ###(df.MAV == '-').sum() ## Testing of replacing hyphen
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while replacing hyphen: ", ex)
df = replace_hyphen(df)
#df.to_excel(r'output.xlsx', index = False)

##11 Drop rows for the columns (AH, MAV, CD) where at least one row value is NULL.
def drop_null_rows(df):
    try:
        print("\n################################################################################")
        print("Q.11 Drop rows for the columns (AH, MAV, CD) where at least one row value is NULL.")
        print("##################################################################################")
        ### Replacing all empty string to Nan for deletion
        df = df.replace(r'', np.NaN, regex=True)
        ## Removing all rows where any value for columns (AH, MAV, CD) is NULL
        df = df.dropna(axis = 0, subset=['AH','MAV','CD'])
        ## After deletion of rows updating other columns back from NaN to empty string
        ## for successful insertion in to SQL server database
        df = df.replace(np.NaN,'', regex=True)
        print("\nSuccessfully deleted all rows for columns (AH, MAV, CD) having row value as NULL")
        return df
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while dropping null rows: ", ex)
df = drop_null_rows(df)

##12 Drop any duplicate rows (except for the first occurrence)
def drop_duplicate_rows(df):
    try:
        print("\n###########################################################")
        print("Q.12 Drop any duplicate rows (except for the first occurrence)")
        print("#############################################################")
        ### Removing duplicate rows keeping the first record alone
        df = df.drop_duplicates(keep='first')
        print("\nSuccessfully removed all duplicate lines")
        return df
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while dropping duplicate rows: ", ex)
df = drop_duplicate_rows(df)

#df.to_excel(r'output2.xlsx', index = False) 
##13 Output the minimum time difference between TOC and ORD.
def calculate_min_time(df):
    try:
        print("\n###########################################################")
        print("Q.13 Output the minimum time difference between TOC and ORD.")
        print("#############################################################")
        ### Creating another column calculating the difference between ORD and TOC
        df['time_delta'] = (pd.to_datetime(df.ORD) - pd.to_datetime(df.TOC)) / pd.Timedelta(seconds=1)
        ## Determining the minimum value for the time difference
        min_time = df['time_delta'].min()
        print("\nThe minimum time difference is: ",str(datetime.timedelta(seconds=min_time)))
        ## Removing the extra column created for difference calculation
        df=df.drop(['time_delta'], axis=1)
        return df
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while calculating minimum time difference: ", ex)
df = calculate_min_time(df)
#df.to_excel(r'output2.xlsx', index = False) 

#####################################################################
## Connecting to SQL server to insert the cleaned data in 
## already created table [Fire_brigade_details]
#####################################################################
def insert_data_into_sql_server(df):
    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-KP4TTDL;'
                      'Database=Python_CA2;'
                      'Trusted_Connection=yes;')
        print("\nConnected to SQL server Successfully")
        cursor = conn.cursor() ## Cursor to access records inside SQL srver
        ## Insert DataFrame records one by one.
        for i,row in df.iterrows():
            SQL = ("INSERT INTO Python_CA2.dbo.Fire_brigade_details "
     "(date,[Station Area],Description,TOC,ORD,MOB,IA,LS,AH,MAV,CD) "
     "VALUES (?,?,?,?,?,?,?,?,?,?,?)")
            VALUES = [row.Date,row['Station Area'],
                  row.Description,row.TOC,row.ORD,row.MOB,row.IA,row.LS,row.AH,row.MAV,row.CD]
            cursor.execute(SQL,VALUES)
        # the connection is not autocommitted by default, so we must commit to save our changes
        conn.commit() 
        ## We should always close the connection after activity
        conn.close()
        print("\n###############################################")
        print("Data inserted into sql server successfully")
        print("n###############################################")
    except Exception as ex: ##catch the error using except block
       print ("\nError has occured while inserting rows in SQL server table: ", ex)
       conn.close()

insert_data_into_sql_server(df)



    
