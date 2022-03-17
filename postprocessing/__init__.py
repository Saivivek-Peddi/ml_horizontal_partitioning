import pathlib
import json
import numpy as np
import pandas as pd

# Getting absolute path of the file
path = str(pathlib.Path(__file__).parent.parent.resolve())

class Postprocess:
    def __init__(self,data,model):
        self.model = model
        data['cluster'] = data.apply(lambda x:self.assign_cluster(x['train_data']),axis=1)
        self.data = data
        with open(f'{path}/data/meta_data/columns.json') as f:
            self.columns = json.load(f)
        self.columns = {v: k for k, v in self.columns.items()}
        self.part_cols = self.create_meta_data()
        with open(f'{path}/data/meta_data/part_cols.json','w') as f:
            json.dump(self.part_cols,f)

    def assign_cluster(self,inp_data):
        return self.model.predict([inp_data])[0]

    def create_meta_data(self):
        df = self.data

        part_cols = {}
        for cluster_id in df['cluster'].unique():
            s = np.sum(df[df['cluster']==cluster_id]['train_data'].to_list(),0)
            # print(cluster_id)
            # print(s)
            # print(np.argmax(s))
            # print(self.columns[np.argmax(s)])
            part_cols[int(cluster_id)] = [self.columns[np.argmax(s)]]
        
        return part_cols


