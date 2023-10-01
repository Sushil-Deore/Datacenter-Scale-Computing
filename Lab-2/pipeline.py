import pandas as pd
import numpy as np
import argparse
from sqlalchemy import create_engine

def extract_data(source):
     
    # Reading csv file
    return pd.read_csv(source)

def transform_data(data):

    # Copying the data
    df = data.copy()

    # Dropping column with 25% & above missing value
    df = df.drop(['Name', 'Outcome Subtype'], axis = 1)

    # Deriving columns "Month" & "Year" from column "MonthYear"
    df[["Month","year"]] = df["MonthYear"].str.split(" ", expand=True)
    
    # Replacing unknown value from Column "Sex upon outcome" by NaN value to new "sex" column
    df["sex"] = df["Sex upon Outcome"].replace("Unknown", np.nan)

    # Dropping columns - "Sex upon outcome"
    df = df.drop(["Sex upon Outcome", "MonthYear"], axis = 1)

    return df

def load_data(data):
    # Exporting dataframe to csv file
    db_url = "postgresql+psycopg2://sushil:hunter2@db:5432/shelter"
    conn = create_engine(db_url)
    data.to_sql("outcomes", conn, if_exists= "append", index=False)

if __name__ == "__main__":
    #parser = argparse.ArgumentParser()
    #parser.add_argument('source', help='source csv')
    #parser.add_argument('target', help='target csv')
    source= "https://data.austintexas.gov/api/views/9t4d-g238/rows.csv"
    #target = "Processed.csv"
    #args = parser.parse_args()

    print("Starting...")
    df = extract_data(source)
    new_df = transform_data(df)
    load_data(new_df)
    
    print("Complete")