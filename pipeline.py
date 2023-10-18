import pandas as pd
import numpy as np
import argparse
from sqlalchemy import create_engine
from sqlalchemy.types import *
    
def extract_data(source):
    # Reading csv file
    return pd.read_csv(source)

def transform_data(data):
    
    # Copying the data
    df = data.copy()
    
    # Replacing unknown value from Column "Sex upon outcome" by NaN value to new "sex" column
    df["Sex"] = df["Sex upon Outcome"].replace("Unknown", np.nan)

    df['ID'] = df['Animal ID']
    df['outcome'] = df['Outcome Type']
    df['Animal'] = df['Animal Type']
    df['Dt'] = df['DateTime']
    df['recorded_name'] = df['Name']
    
    cols_to_drop = ['Outcome Type','Animal Type','DateTime','Animal ID','Name']
    df.drop(cols_to_drop,axis=1,inplace=True)

    # Transforming dataframe to match schema of warehouse

    df_outcome = df['outcome'].drop_duplicates().reset_index()
    df_outcome['outcome_id'] = df_outcome.index + 1
    df_outcome.drop('index',axis=1,inplace=True)
    df = df.merge(df_outcome)

    df_animal = df['Animal'].drop_duplicates().reset_index()
    df_animal['Animal_id'] = df_animal.index + 1
    df_animal.drop('index',axis=1,inplace=True)
    df = df.merge(df_animal)

    df_breed = df['Breed'].drop_duplicates().reset_index()
    df_breed['Breed_id'] = df_breed.index + 1
    df_breed.drop('index',axis=1,inplace=True)
    df = df.merge(df_breed)

    df_date = df['Dt'].drop_duplicates().reset_index()
    df_date['date_id'] = df_date.index + 1
    df_date.drop('index',axis=1,inplace=True)
    df[["Month", "Year"]] = df["Dt"].str.split(" ", n=1, expand=True)
    df[["Month", "Year"]] = df[["Month", "Year"]].drop_duplicates()

    df = df.merge(df_date)
    
    # Retaining the useful columns
    cols = ['recorded_name','date_id','outcome_id','Animal_id','Sex','Breed_id','Color','ID']

    df = df[cols]
    
    return df, df_animal,df_date,df_outcome,df_breed

def load_data(data):
    
    db_url = "postgresql+psycopg2://sushil:hunter2@db:5432/shelter"

    engine = create_engine(db_url)

    df, df_animal,df_date,df_outcome,df_breed = data

    df.to_sql(name= 'ADOPTION',con=engine, if_exists='append',index=False)
    
    df_animal.to_sql(name='ANIMAL',con=engine, if_exists='append',index=False)
 
    df_outcome.to_sql(name='OUTCOME',con=engine, if_exists='append',index=False)

    df_breed.to_sql(name='BREED',con=engine, if_exists='append',index=False)

    df_date.to_sql(name='DATE',con=engine, if_exists='append',index=False)
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()

    parser.add_argument('source', help='source csv')
    
    args = parser.parse_args()

    print("Starting...")
    
    df = extract_data(args.source)

    new_df = transform_data(df)
    
    load_data(new_df)
    
    print("Complete!!!")
