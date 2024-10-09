from orchestrator import Orchestrator
import pandas as pd

my_orchestrator = Orchestrator()
files_to_process = {'../data/Licencias_Locales_202104.csv':'../output/Licencias_SinDuplicados.csv'
                        ,'../data/Terrazas_202104.csv':'../output/Terrazas_Normalizadas.csv'
                        ,'../data/Locales_202104.csv':'../output/Locales_Procesado.csv'
                        ,'../data/books.json':'../output/Books_Limpio.csv'}

my_orchestrator.process_in_batch(files=files_to_process)

#Data integration
## Join entre datasets
df_terr = pd.read_csv('../output/Terrazas_Normalizadas.csv')
df_lic = pd.read_csv('../output/Licencias_SinDuplicados.csv')
df_joined = my_orchestrator.join_2_datasets(df_terr,df_lic)
df_joined.to_csv('../output/Licencias_Terrazas_Integradas.csv')

#Calculo area superficie 
#Se calcula utilizando la columna Superficie_Es de el archivo de terrazas
#en este caso se toma el que ya ha sido pre-procesado 
df_superficies_agg = (df_terr
                          .groupby(by=['id_barrio_local'])
                          .agg(superficie_total=('Superficie_ES','sum'))
                          .reset_index()
                          .sort_values(by=['superficie_total'],ascending = False)
                          )
df_superficies_agg.to_csv('../output/Superficies_Agregadas.csv',index=False)