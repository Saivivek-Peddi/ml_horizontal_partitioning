import pathlib
import json
import numpy as np
import pandas as pd
import joblib
from kmodes.kmodes import KModes

# Getting absolute path of the file
path = str(pathlib.Path(__file__).parent.parent.resolve())

class Training:
    def __init__(self,training_data,n_clusters):
        self.training_data = training_data['train_data']
        self.n_clusters = n_clusters
        self.model = self.generate_model(self.training_data,self.n_clusters)
        self.save_model(self.model)

    def generate_model(self,train_data,clusters):
        print('')
        print('Starting Training')
        km = KModes(n_clusters=clusters, init='Huang', n_init=5, verbose=1)
        model = km.fit(train_data.to_list())

        print('')
        print('Completed Training')
        return model
        

    def save_model(self,model):
        # save the model to disk
        print('')
        print('Saving the model')
        filename = f'{path}/models/finalized_model.sav'
        joblib.dump(model, filename)
 
# some time later...
 
# load the model from disk
# loaded_model = joblib.load(filename)
# result = loaded_model.score(X_test, Y_test)
# print(result)