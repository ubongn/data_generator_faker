from faker import Faker
import random

fake = Faker(locale="de")
import pandas as pd
import polars as pl


def generate_employee(rows, path, type='csv'):
    
    department = ["IT", "Marketing", "HR", "Finance"]
    role = ["Manager", "Developer", "Analyst", "Associate"]
    
    collect_data = {
        "first_name": [fake.first_name() for i in range(rows)],
        "last_name": [fake.last_name() for i in range(rows)],
        "job": [fake.job() for i in range(rows)],
        "department": [random.choice(department) for i in range(rows)],
        "role": [random.choice(role) for i in range(rows)],
        "salary": [fake.random_int(min=30000, max=150000, step=1000) for i in range(rows)],
        "email": [fake.email() for i in range(rows)],
    }
    
    
    if (type == "csv") :
        df = pd.DataFrame(collect_data)
        df.to_csv(path, index=False)
    elif (type == "parquet") :
        df = pd.DataFrame(collect_data)
        df.to_parquet(path, index=False)
    elif (type == "delta") :
        df = pl.DataFrame(collect_data)
        df.write_delta(path, mode="overwrite")
    else:
        df = pd.DataFrame(collect_data)
        df.to_csv(path, index=False)
        

def generate_person(rows, path, type='csv'):
    item_selection = ["M_STAN_Q","M_STAN_K","M_STAN_T","M_PREM_Q","M_STAN_F","M_PREM_F","M_PREM_T","M_PREM_K","P_DOWN_S","P_FOAM_S","P_FOAM_K","P_DOWN_K"]
    item_name = ["Standard Queen Mat","Standard King Mat","Standard Twin Mat","Premium Queen Mat","Standard Full Mat","Premium Full Matt","Premium Twin Matt"]
          
    collect_data = {
        "item_id" : [random.choice(item_selection) for i in range(rows)],
        "item_name": [random.choice(item_name) for i in range(rows)],
        "price": [fake.random_number(digits=3) for i in range(rows)],

    }
    
    if (type == "csv") :
        df = pd.DataFrame(collect_data)
        df.to_csv(path, index=False)
    elif (type == "parquet") :
        df = pd.DataFrame(collect_data)
        df.to_parquet(path, index=False)
    elif (type == "delta") :
        df = pl.DataFrame(collect_data)
        df.write_delta(path, mode="overwrite")
    else:
        df = pd.DataFrame(collect_data)
        df.to_csv(path, index=False)


       
def generate_sales(rows, path ,type="csv") :
    
    values = list (range(2777, 3000))
   
    collect_data = {
        "order_id" : [random.choice(values) for i in range(rows)], 
        "email" : [fake.email() for i in range(rows)], 
    }
    
    if (type == "csv") :
        df = pd.DataFrame(collect_data)
        df.to_csv(path, index=False)
    elif (type == "parquet") :
        df = pd.DataFrame(collect_data)
        df.to_parquet(path, index=False)
    elif (type == "delta") :
        df = pl.DataFrame(collect_data)
        df.write_delta(path, mode="overwrite")
    else:
        df = pd.DataFrame(collect_data)
        df.to_csv(path, index=False)