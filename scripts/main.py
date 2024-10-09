from preprocess import *


class Orchestrator():
    def __init__(self) -> None:
        pass

    def process_one_file(self,src_path,sink_path):
        if 'licencias' in src_path.lower():
            #processing the Licencias file
            df_licencias = (Licencias(path=src_path)
                            .process_all())
            df_licencias.to_csv(sink_path,index=False)
            return sink_path
        elif 'locales' in src_path.lower():
            #processing the Lcoales file
            df_locales = Locales(path=src_path).process_all()
            df_locales.to_csv(sink_path,index=False)
            return sink_path
        elif 'terrazas' in src_path.lower():
            #processing the Terrazas file
            df_terrazas = Terrazas(path=src_path).process_all()
            df_terrazas.to_csv(sink_path,index=False)
            return sink_path
        elif 'books' in src_path.lower():
            #processing the books file
            df_books = Books(path=src_path).process_all()
            df_books.to_csv(sink_path,index=False)
            return sink_path
        else:
            return None
        
    def process_in_batch(self,files):
        for src_path in list(files.keys()):
            sink_path = files[src_path]
            self.process_one_file(src_path=src_path,sink_path=sink_path)
        print('All files was processed')

    def join_2_datasets(self,df1,df2):
        df1_ = df1.copy()
        df2_ = df2.copy()
        key = 'id_local'

        df1_[key] = df1_[key].astype(int)
        df2_[key] = df2_[key].astype(int)

        return df1_.join(df2_.set_index(key),on=key,lsuffix='Terrazas',rsuffix='Licencias',how='inner')



        
if __name__=='__main__':
    orchestrator = Orchestrator()
    files_to_process = {'../data/Licencias_Locales_202104.csv':'../output/Licencias_SinDuplicados.csv'
                        ,'../data/Terrazas_202104.csv':'../output/Terrazas_Normalizadas.csv'
                        ,'../data/Locales_202104.csv':'../output/Locales_Procesado.csv'
                        ,'../data/books.json':'../output/Books_Limpio.csv'}
    orchestrator.process_in_batch(files=files_to_process)

    #Data integration
    ## Join entre datasets
    df_terr = pd.read_csv('../output/Terrazas_Normalizadas.csv')
    df_lic = pd.read_csv('../output/Licencias_SinDuplicados.csv')
    df_joined = orchestrator.join_2_datasets(df_terr,df_lic)
    df_joined.to_csv('../output/Licencias_Terrazas_Integradas.csv')

    #Datos Geograficos
    df_superficies_agg = (df_terr
                          .groupby(by=['id_barrio_local'])
                          .agg(superficie_total=('Superficie_ES','sum'))
                          .reset_index()
                          .sort_values(by=['superficie_total'],ascending = False)
                          )
    df_superficies_agg.to_csv('../output/Superficies_Agregadas.csv',index=False)


