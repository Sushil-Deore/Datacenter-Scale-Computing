import pandas as pd
import numpy as np
from pathlib import Path

outcomes_map = {'Rto-Adopt' : 1,
                'Adoption': 2,
                'Euthanasia': 3,
                'Transfer': 4,
                'Return to Owner': 5,
                'Died': 6,
                'Disposal' : 7, 
                'Missing': 8,
                'N/A': 9,
                'Relocate': 10,
                'Stolen': 11}

def transform_data(source_csv, target_dir):
    new_data = pd.read_csv(source_csv)
    new_data = prep_data(new_data)

    dim_dates = prep_date_dim(new_data)
    dim_outcome_types = prep_outcomes_type_dim(new_data)
    dim_breed = prep_breed_dim(new_data)
    dim_color = prep_color_dim(new_data)
    dim_animal_type = prep_animal_type_dim(new_data)
    dim_sex = prep_sex_dim(new_data)

    fact_outcomes = prep_outcomes_fact(new_data)

    Path(target_dir).mkdir(parents=True, exist_ok=True)

    dim_dates.to_parquet(target_dir+'/dim_dates.parquet')
    dim_outcome_types.to_parquet(target_dir+'/dim_outcome_types.parquet')
    dim_breed.to_parquet(target_dir+'/dim_breed.parquet')
    dim_color.to_parquet(target_dir+'/dim_color.parquet')
    dim_animal_type.to_parquet(target_dir+'/dim_animal_type.parquet')
    dim_sex.to_parquet(target_dir+'/dim_sex.parquet')

    fact_outcomes.to_parquet(target_dir+'/fact_outcomes.parquet')

def prep_data(data):

    data['name'] = data['Name'].str.replace("*", "", regex=False)

    data['sex'] = data["Sex upon Outcome"].replace({"Neutered Male": "M", 
                                                   "Intact Male": "M",
                                                   "Intact Female": "F",
                                                   "Spayed Female": "F",
                                                   "Unknown": np.nan})
    
    data["is_fixed"] = data['Sex upon Outcome'].replace({"Neutered Male": True, 
                                                        "Intact Male": False,
                                                        "Intact Female": False,
                                                        "Spayed Female": True,
                                                        "Unknown": np.nan})

    data['ts'] = pd.to_datetime(data.DateTime)
    data['date_id'] = data.ts.dt.strftime('%Y%m%d')
    data['time']= data.ts.dt.time

    data['outcome_type_id'] = data['Outcome Type'].fillna('N/A')
    data['outcome_type_id'] = data['Outcome Type'].replace(outcomes_map)

    return data

def prep_breed_dim(data):
    # Extracting breed dim
    breed_dim = pd.DataFrame({'Breed': data.Breed})
    return breed_dim.drop_duplicates()

def prep_color_dim(data):
    # Extracting breed dim
    color_dim = pd.DataFrame({'Color': data.Color})
    return color_dim.drop_duplicates()

def prep_animal_type_dim(data):
    animal_type_dim = pd.DataFrame({'Animal Type': data['Animal Type']})
    return animal_type_dim.drop_duplicates()

def prep_sex_dim(data):
    sex_dim = pd.DataFrame({'Sex upon Outcome': data['Sex upon Outcome']})
    return sex_dim.drop_duplicates()

def prep_date_dim(data):

    data.DateTime = pd.to_datetime(data.DateTime)

    dates_dim = pd.DataFrame({'date': data.DateTime,
                            'year': data.DateTime.dt.year,
                            'month': data.DateTime.dt.month,
                            'day': data.DateTime.dt.day})

    # Drop Duplicates date records
    return dates_dim.drop_duplicates()

def prep_outcomes_type_dim(data):
    
    outcome_types_dim = pd.DataFrame.from_dict(outcomes_map, orient='index').reset_index()
    
    outcome_types_dim.columns=['outcome_type','outcome_type_id']
    return outcome_types_dim.drop_duplicates()

def prep_outcomes_fact(data):
    outcomes_fact = data[["Animal ID", 'name', 'is_fixed']]
    outcomes_fact= outcomes_fact.rename(columns = {'Animal ID': "animal_id"})
    outcomes_fact = outcomes_fact.dropna()
    return outcomes_fact.drop_duplicates()