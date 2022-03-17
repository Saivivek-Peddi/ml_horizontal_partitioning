from sklearn.cluster import KMeans
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from matplotlib import pyplot as plt
# %matplotlib inline

df = pd.read_csv("data.csv")

km = KMeans(n_clusters=2)
y_predicted = km.fit_predict(df[['x','y']])
y_predicted

import pandasql as ps
ps.sqldf("""select * from df where State='CA'""")

import pandas as pd
df = pd.read_csv('/Users/saivivekpeddi/Downloads/Coding/Courses/ECS251_OS/ml_horizontal_partitioning/data/actual_data/StateNames.csv',nrows= 100)
del df['Id']
df.to_csv('/Users/saivivekpeddi/Downloads/Coding/Courses/ECS251_OS/ml_horizontal_partitioning/data/actual_data/StateNames.csv',index=False)
