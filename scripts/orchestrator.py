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
        print('All datasets were processed Successfully!')

    def join_2_datasets(self,df1,df2):
        df1_ = df1.copy()
        df2_ = df2.copy()
        key = 'id_local'

        df1_[key] = df1_[key].astype(int)
        df2_[key] = df2_[key].astype(int)

        return df1_.join(df2_.set_index(key),on=key
                         ,lsuffix='Terrazas'
                         ,rsuffix='Licencias'
                         ,how='inner')   


