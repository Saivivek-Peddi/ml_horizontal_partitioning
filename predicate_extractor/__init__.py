import pathlib
import json
from sql_metadata import Parser

# Getting absolute path of the file
path = str(pathlib.Path(__file__).parent.parent.resolve())


class Predicates:
    def __init__(self,query):
        self.parser = Parser(query)
        self.query = query
        with open(f'{path}/data/meta_data/columns.json') as f:
            self.columns = json.load(f)
        self.training_data = self.get_training_list()

    def get_training_list(self):
        out_list = [0]*len(self.columns.keys())
        try:
            if 'where' in self.parser.columns_dict:
                for item in self.parser.columns_dict['where']:
                    out_list[self.columns[item]]=1
        except Exception as E:
            print(E)
            print(self.query)
        return out_list

