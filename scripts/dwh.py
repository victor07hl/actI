import pandas as pd
from Connections import connections, sql_management
import json
from DataProcess import main, build_dwh

#Processing the data
load_process = main()
load_process.execute_all()

#creating database and schema if not exists
sql_m = sql_management()
db = 'dwh'
sh = 'terrazas'
sql_m.create_database(db=db)
sql_m.create_schema(db,sh)

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
dwh_create = build_dwh()
dwh_create.write_fact_and_dims(df_selected=df_selected
                               ,dimensions=dimensions
                               ,db=db
                               ,sh=sh
                               ,fact_name='fact_terrazas')



