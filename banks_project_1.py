# Code for ETL operations on Largest Banks data

# Importing the required libraries
import requests
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(f"{timestamp} : {message}\n")

def extract(url, table_attribs):
    ''' This function aims to extract the required information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    banks_data = []
    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 3:
            banks_info = {
                "Name": cols[1].get_text(strip=True),
                "MC_USD_Billion": cols[2].get_text(strip=True).replace(',', '')
            }
            banks_data.append(banks_info)
            if len(banks_data) == 10:
                break
    df = pd.DataFrame(banks_data)
    return df

def transform(df1, csv_path):
    ''' This function accesses the CSV file for exchange rate information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to respective currencies '''

    # Read exchange rate CSV
    dataframe = pd.read_csv(csv_path)
    
    # Convert it into a dictionary like {'EUR': 0.93, 'GBP': 0.8, 'INR': 82.95}
    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']
    # Convert market cap to float if it's not already
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)

    # Create new currency columns
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
 
    return df

def load_to_csv(df2, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    log_progress("Data successfully written to CSV.")

def load_to_db(df3, sql_connection, table_name):
    ''' This function saves the final dataframe to as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_queries(query1,query2,query3, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print("\n=== Query 1: All Records ===")
    print(query1)
    result1 = pd.read_sql(query1, sql_connection)
    print(result1)

    print("\n=== Query 2: Average GBP Market Cap ===")
    print(query2)
    result2 = pd.read_sql(query2, sql_connection)
    print(result2)

    print("\n=== Query 3: Top 5 Bank Names ===")
    print(query3)
    result3 = pd.read_sql(query3, sql_connection)
    print(result3)



# Required variables
url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attributes = ["Name", "MC_USD_Billion"]
csv_path = '/home/project/exchange_rate.csv'
output_path = "Largest_banks_data.csv"
db_name = 'Banks.db'
table_name = 'Largest_banks'
sql_connection = sqlite3.connect(db_name)


# ----------- ETL Process Execution -----------

log_progress("ETL Job Started")  # Start log

log_progress("Extract phase Started")
df = extract(url, table_attributes)  # Run extraction
log_progress("Extract phase Ended")

log_progress("Transform phase Started")
df1 = transform(df, csv_path)  # Run transformation
print("Transformed Data")
print(df1)  # Display transformed data in console
log_progress("Transform phase Ended")

log_progress("Load cvs Started")
df2 = load_to_csv(df1,output_path)  # Save transformed data
log_progress("Load csv Ended")

log_progress("Load db Started") # Load to DB
df3 = load_to_db(df2,sql_connection,table_name)
log_progress("Load db Ended")

log_progress("run query Started") # Verify by running query
query1 = f"SELECT * FROM {table_name}"
query2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
query3 = f"SELECT Name FROM {table_name} LIMIT 5"

# Run the queries
run_queries(query1, query2, query3, sql_connection)

# Close connection
sql_connection.close()
log_progress("run query Ended")
log_progress("ETL Job Finished")
