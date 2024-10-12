from orchestrator import Orchestrator
import pandas as pd
import json
from Connections import connections
from datetime import datetime

class main():
    def __init__(self):
        pass
    def execute_all(self):
        my_orchestrator = Orchestrator()

        def cast_str_2_date(date):
            splited = date.split('/')
            year = int(splited[2])
            month = int(splited[1])
            day = int(splited[0])
            dt = datetime(year,month,day)
            return dt.date()

        #Loading the variables
        with open('../configs/variables.json','r') as file:
            vars = json.load(file)

        all_src = vars['sources']
        all_out = vars['output']
        src_to_proccess = ['Licencias_locales','Terrazas','Locales','books']
        out_to_store = ['Licencias_SinDuplicados','Terrazas_Normalizadas','Lcoales_Procesado','Books_Limpio']

        src_to_proccess_paths = [all_src[key] for key in src_to_proccess]
        out_to_store_paths = [all_out[key] for key in out_to_store]
        files_to_process = dict(zip(src_to_proccess_paths,out_to_store_paths))


        my_orchestrator.process_in_batch(files=files_to_process)

        #Data integration
        ## Join entre datasets
        df_terr = pd.read_csv(all_out['Terrazas_Normalizadas'])
        df_lic = pd.read_csv(all_out['Licencias_SinDuplicados'])
        df_joined = my_orchestrator.join_2_datasets(df_terr,df_lic)
        #casting the dates
        df_joined['Fecha_confir_ult_decreto_resol'] = (df_joined["Fecha_confir_ult_decreto_resol"]
                                            .apply(cast_str_2_date))
        df_joined['Fecha_Dec_Lic'] = (df_joined["Fecha_Dec_Lic"]
                                            .apply(cast_str_2_date))
        df_joined.to_csv(all_out['Licencias_Terrazas_Integradas'],index=False)

        #Calculo area superficie 
        #Se calcula utilizando la columna Superficie_Es de el archivo de terrazas
        #en este caso se toma el que ya ha sido pre-procesado 
        df_superficies_agg = (df_terr
                                .groupby(by=['id_barrio_local'])
                                .agg(superficie_total=('Superficie_ES','sum'))
                                .reset_index()
                                .sort_values(by=['superficie_total'],ascending = False)
                                )
        df_superficies_agg.to_csv(all_out['Superficies_Agregadas'],index=False)

class build_dwh(connections):
    def __init__(self):
        super().__init__
    
    def write_fact_and_dims(self,df_selected,tables,db,sh):
        #Writing the dimensions
        cnxn = self.engine(db=db)
        dim_agg_cols = []
        for tbl in tables:
            tbl_col = tables[tbl]
            df = df_selected[tbl_col]
            if tbl.startswith('dim'):
                df_ = df.drop_duplicates().copy()
                print(f'dimension saved as {tbl}')
            else:
                df_ = df.copy()
                
            df_.to_sql(name=tbl,con=cnxn,schema=sh,if_exists='replace')
            print(f'fact saved as {tbl}')

if __name__=='__main__':
    load_process = main()
    load_process.execute_all()

    