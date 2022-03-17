import pathlib
import json
import pandas as pd
from predicate_extractor import Predicates

# Getting absolute path of the file
path = str(pathlib.Path(__file__).parent.parent.resolve())

class Preporcess:
    def __init__(self,db_name,queries_file_name):
        self.data_path = f'{path}/data/actual_data/{db_name}'
        self.columns = self.get_columns(self.data_path)
        self.write_columns_meta_data(self.columns)
        self.training_data = self.gen_training_data(queries_file_name)
        print('Generated Trainig Data')

    def get_columns(self,data_path):
        df = pd.read_csv(data_path,nrows=2)
        columns = list(df.columns)
        out = {}
        for i in range(len(columns)):
            out[columns[i]]=i
        return out
    
    def write_columns_meta_data(self,columns):
        with open(f'{path}/data/meta_data/columns.json','w') as f:
            json.dump(columns,f)
    
    def gen_training_data(self,queries_file_name):
        train_path = f'{path}/data/queries_for_training/{queries_file_name}'
        print(' ')
        print('Generating Training Data')
        df = pd.read_csv(train_path)
        df['train_data'] = df.apply(lambda x: self.get_unique_set(x['query']), axis=1)
        return df
        
    def get_unique_set(self,query):
        p = Predicates(query)
        return p.training_data





