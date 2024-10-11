import pandas as pd
from Connections import connections, sql_management
import json
from DataProcess import main

#Processing the data
load_process = main()
load_process.execute_all()

#loading the connection for pandas
cnxn = connections().engine(db='dwh')

#creating database and schema if not exists
sql_m = sql_management()
sql_m.create_database(db='dwh')
sql_m.create_schema('dwh','terrazas')

#getting the variables
with open('../configs/variables.json','r') as file:
    vars = json.load(file)

all_out = vars['output']
#df = pd.read_csv('../output/Licencias_Terrazas_Integradas.csv')
df = pd.read_csv(all_out['Licencias_Terrazas_Integradas'])

#building the fact Table 
#removing the duplicate columns from Licencias
select_cols = [col for col in df.columns if col.endswith('Licencias')!=True]
df_selected = df[select_cols]
#Removing Terrazas from the cols name
clean_cols = [col_.replace('Terrazas','') for col_ in select_cols]
df_selected.columns = clean_cols

#Loading the dimensions File
with open('../configs/terrazas_dimensions.json','r') as file:
    dimensions = json.load(file)

#Writing the dimensions
dim_agg_cols = []
for dim in dimensions:
    dim_col = dimensions[dim]
    df = df_selected[dim_col]
    df_ = df.drop_duplicates().copy()
    dim_name = f"dim_{dim}"
    df_.to_sql(name=dim_name,con=cnxn,schema='terrazas',if_exists='replace')
    [dim_agg_cols.append(col_) for col_ in dim_col if col_.startswith('id')!=True]
    print(dim_name,"saved")

#Writing the Fact Table
fact_cols = [fact_col for fact_col in df_selected.columns if fact_col not in dim_agg_cols]
df_fact = df_selected[fact_cols]
df_fact.to_sql(name='Fact_terrazas',con=cnxn,schema='terrazas',if_exists='replace')
print('Fact_terrazas saved')



