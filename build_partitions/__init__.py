import pathlib
import json
import numpy as np
import pandas as pd

# Getting absolute path of the file
path = str(pathlib.Path(__file__).parent.parent.resolve())

# Avoid small file problem
class Build_partitions:
    def __init__(self,data_path):
        self.data_path = data_path
        with open(f'{path}/data/meta_data/part_cols.json') as f:
            self.part_cols = json.load(f)
        self.df = pd.read_csv(self.data_path,nrows=10000)
        pathlib.Path(f'{path}/data/partitioned_data').mkdir(parents=True, exist_ok=True)
        self.part_meta_data = []
        self.generate_partitions()
        with open(f'{path}/data/meta_data/part_meta_data.json','w') as f:
            json.dump(self.part_meta_data,f,indent=4)
    
    def generate_partitions(self):
        df = self.df
        self.vertical_partitioning(df)
        for key,value in self.part_cols.items():
            print(key,value)
            if len(df[value[0]].unique())>200:
                self.lexographic_partitioning(df,key,value[0])
            else:
                self.value_based_partitioning(df,key,value[0])
            
    
    def value_based_partitioning(self,df,cluster_id,col):
        print(f'Started Value Partitioning Based on {col}')
        meta_data = {}
        meta_data['cluster_id'] = cluster_id
        meta_data['partitoned_by'] = [col]
        meta_data['base_path'] = f'{path}/data/partitioned_data/{col}'
        pathlib.Path(meta_data['base_path']).mkdir(parents=True, exist_ok=True)
        meta_data['meta'] = {}

        for part in df[col].unique():
            part2 = part
            part = str(part)
            print(part)
            meta_data['meta'][part] = {}
            meta_data['meta'][part]['path'] = meta_data['base_path']+f'/{part}/part_{part}'
            pathlib.Path(meta_data['base_path']+f'/{part}').mkdir(parents=True, exist_ok=True)
            
            meta_data['meta'][part]['rows'] = df[df['part']==part].shape[0]
            meta_data['meta'][part]['cols'] = df[df['part']==part].shape[1]
            # df[df[col]==part2].to_parquet(meta_data['meta'][part]['path'])
            df[df[col]==part2].to_csv(meta_data['meta'][part]['path']+'.csv',index=False)

        self.part_meta_data.append(meta_data)

    def lexographic_partitioning(self,df,cluster_id,col):
        print(f'Started Lexographic Partitioning Based on {col}')
        meta_data = {}
        meta_data['cluster_id'] = cluster_id
        meta_data['partitoned_by'] = [col]
        meta_data['base_path'] = f'{path}/data/partitioned_data/{col}'
        pathlib.Path(meta_data['base_path']).mkdir(parents=True, exist_ok=True)
        meta_data['meta'] = {}

        df['part'] = df.apply(lambda x: x[col][0],axis=1)
        for part in df['part'].unique():
            part = str(part)
            print(part)
            meta_data['meta'][part] = {}
            meta_data['meta'][part]['path'] = meta_data['base_path']+f'/{part}/part_{part}'
            pathlib.Path(meta_data['base_path']+f'/{part}').mkdir(parents=True, exist_ok=True)
            meta_data['meta'][part]['rows'] = df[df['part']==part].shape[0]
            meta_data['meta'][part]['cols'] = df[df['part']==part].shape[1]
            
            # df[df['part']==part].to_parquet(meta_data['meta'][part]['path'])
            df[df['part']==part].to_csv(meta_data['meta'][part]['path']+'.csv',index=False)

        self.part_meta_data.append(meta_data)

    def vertical_partitioning(self,df):
        print(f'Started Vertical Partitioning')
        meta_data = {}
        meta_data['cluster_id'] = -1
        meta_data['partitoned_by'] = []
        meta_data['base_path'] = f'{path}/data/partitioned_data/vertical'
        pathlib.Path(meta_data['base_path']).mkdir(parents=True, exist_ok=True)
        meta_data['meta'] = {}
        
        part = 'vertical'
        meta_data['meta'][part] = {}
        meta_data['meta'][part]['path'] = meta_data['base_path']+f'/{part}/part_{part}'
        pathlib.Path(meta_data['base_path']+f'/{part}').mkdir(parents=True, exist_ok=True)
        meta_data['meta'][part]['rows'] = df.shape[0]
        meta_data['meta'][part]['cols'] = df.shape[1]
        # df.to_parquet(meta_data['meta'][part]['path'])
        df.to_csv(meta_data['meta'][part]['path']+'.csv',index=False)
        
        self.part_meta_data.append(meta_data)
