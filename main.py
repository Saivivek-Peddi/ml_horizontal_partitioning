from preprocessing import Preporcess
from training import Training
from postprocessing import Postprocess
from build_partitions import Build_partitions

prp = Preporcess('StateNames.csv','queries.csv')
t = Training(prp.training_data,3)
pop = Postprocess(prp.training_data,t.model)
b = Build_partitions(prp.data_path)
