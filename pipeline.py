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

    df = df.rename(columns={'Animal ID': 'animal_id', 
                    'Outcome Type': 'outcometype', 
                    'Animal Type' : 'animaltype', 
                    'DateTime' : 'datetime',
                    'Name': 'name',
                    'Breed': 'breed',
                    'Sex': 'sex', 
                    'Color': 'color'})

    # Transforming dataframe to match schema of warehouse

    df_outcometype = df['outcometype'].drop_duplicates().reset_index()
    df_outcometype.drop('index',axis=1,inplace=True)

    df_animaltype = df['animaltype'].drop_duplicates().reset_index()
    df_animaltype.drop('index',axis=1,inplace=True)

    df_breed = df['breed'].drop_duplicates().reset_index()
    df_breed.drop('index',axis=1,inplace=True)

    df_date = df['datetime'].drop_duplicates().reset_index()
    df_date.drop(['index'],axis=1,inplace=True)

    df_sex = df['sex'].drop_duplicates().reset_index()
    df_sex.drop('index',axis=1,inplace=True)

    df_color = df['color'].drop_duplicates().reset_index()
    df_color.drop('index',axis=1,inplace=True)

    cols = ['name', 'animal_id']
    df = df[cols]

    return df_outcometype, df_animaltype,df_breed, df_date, df_sex, df_color, df

def load_data(data):
    
    db_url = "postgresql+psycopg2://sushil:hunter2@db:5432/shelter"

    engine = create_engine(db_url)

    df_outcometype, df_animaltype, df_breed, df_date, df_sex, df_color, df = data
 
    df_outcometype.to_sql(name='outcometypedim',con=engine, if_exists='append',index=False)

    df_animaltype.to_sql(name='animaltypedim',con=engine, if_exists='append',index=False)

    df_breed.to_sql(name='breeddim',con=engine, if_exists='append',index=False)

    df_sex.to_sql(name='sexdim',con=engine, if_exists='append',index=False)

    df_color.to_sql(name='colordim',con=engine, if_exists='append',index=False)

    df_date.to_sql(name='datedim',con=engine, if_exists='append',index=False)

    df.to_sql(name= 'adoption',con=engine, if_exists='append',index=False)
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()

    parser.add_argument('source', help='source csv')
    
    args = parser.parse_args()

    print("Starting...")
    
    df = extract_data(args.source)

    new_df = transform_data(df)
    
    load_data(new_df)
    
    print("Complete!!!")
