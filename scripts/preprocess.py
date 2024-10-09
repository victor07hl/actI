import pandas as pd
import numpy as np
import re

class common_processing():
    def __init__(self) -> None:
        pass
    def rows_filtered(self,df,threshold):
        threshold_ = len(df.columns) * threshold
        df_ = df.copy()
        return df.dropna(thresh=threshold_)
    
    def replace_value(self,df,col,old_value,new_value):
        df_ = df.copy()
        df_[col] = df_[col].apply(lambda x:x.replace(old_value,new_value))
        return df_
    
    def load_file(self,path,sep,encoding):
        return pd.read_csv(path,sep=sep,encoding=encoding)
    
    def drop_duplicates(self,df,subset):
        df_ = df.copy()
        return df_.drop_duplicates(subset=subset)
    
    def clean_row_coordenadas(self,row_coordenada):
        if row_coordenada == None:
            return np.nan
        else:
            return str(row_coordenada).replace(',','.')
    
    def clean_ids(self,id):
        try:
            return str(id).split('.')[0]
        except AttributeError as e:
            return id
        
    def clean_col_coordenadas(self,df):
        df_ = df
        for col,type in df_.dtypes.items():
            if 'coordenada' in col:
                df_[col] = (df_[col]
                              .apply(self.clean_row_coordenadas)
                              .astype(float)
                            )
        return df_
    
    def remove_long_spaces(self,row_string):
        try:
            return re.sub(r'\s+',' ',row_string).strip()
        except TypeError as e:
            return None

class Terrazas(common_processing):
    def __init__(self,path) -> None:
        self.path = path
        super().__init__()
    
    def divide_cols(self,df,quotient_col,numerator_col,denominator_col):
        df_ = df.copy()
        df_[quotient_col] = df_[numerator_col]/df_[denominator_col]
        return df_
    
    def process_all(self):
        df = self.load_file(path=self.path,sep=';',encoding='iso8859_2')
        df_cleaned = self.rows_filtered(df=df, threshold=0.5)
        df_cleaned['Superficie_ES'] = (df_cleaned['Superficie_ES']
                                       .apply(lambda x: x.replace(',','.'))
                                       .astype(float)
                                        )
        df_with_div = self.divide_cols(df_cleaned,'Terrazas_Normalizadas','Superficie_ES','id_terraza')
        return df_with_div
    
class Licencias(common_processing):
    def __init__(self,path) -> None:
        self.path = path
        super().__init__()
    
    def process_all(self):
        df = self.load_file(path=self.path,sep=';',encoding='utf-8')
        df_cleaned = self.rows_filtered(df=df,threshold=0.5)
        df_no_duplicates = self.drop_duplicates(df_cleaned,['id_local','ref_licencia'])
        df_clean_coor = self.clean_col_coordenadas(df=df_no_duplicates)

        return df_clean_coor

class Locales(common_processing):
    def __init__(self,path) -> None:
        self.path = path
        super().__init__()

    def clean_cols_ids(self,df):
        ids_to_be_clean = ["id_agrupacion","id_tipo_agrup","id_local_agrupado"]
        df_=df
        for col_ in ids_to_be_clean:
            df_[col_] = df_[col_].apply(self.clean_ids)
        return df_


    def process_all(self):
        df = self.load_file(path=self.path,sep=';',encoding='iso8859_1')
        df_cleaned = self.rows_filtered(df=df,threshold=0.5)
        df_cleaned = self.clean_cols_ids(df=df_cleaned)

        df_cleaned_out = self.clean_col_coordenadas(df_cleaned)

        return df_cleaned_out

class Books(common_processing):
    def __init__(self,path) -> None:
        self.path = path
        super().__init__()

    def clean_date(self,row_date_):
        try:
            return pd.to_datetime(row_date_['$date'])
        except TypeError as e:
            return None

    def process_all(self):
        df = pd.read_json(self.path,lines=True)

        #Elimna el campo _id
        df = df.drop('_id',axis=1)

        #Excluye los libros que no tienen ISBN
        df = df[~df['isbn'].isna()]

        #Transforming data , doing exploding to array columns
        df = df.explode('authors')
        df = df.explode('categories')
        df.reset_index(inplace=True)

        #getting the publishedDate as Datetime
        df['publishedDate']= df['publishedDate'].apply(self.clean_date)

        #Removing long spaces
        df['longDescription'] = df['longDescription'].apply(self.remove_long_spaces)
        df['shortDescription'] = df['shortDescription'].apply(self.remove_long_spaces)

        return df.copy()




