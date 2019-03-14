import feather
import multiprocessing

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.pyplot as plt


from scipy import sparse

import pandas as pd
from scipy import sparse

from sklearn.cluster import KMeans, MiniBatchKMeans, DBSCAN
from sklearn.metrics import silhouette_samples, silhouette_score
from sklearn.preprocessing import Normalizer, MaxAbsScaler

#%%

## Load Data
path = 'data/processed/EDY/returns_s.feather'
returns = feather.read_dataframe(path)
returns.shape

path = 'data/processed/EDY/holdings_summary_s_s.feather'
summary = feather.read_dataframe(path)
summary.shape

path = 'data/processed/EDY/holdings_s_s.npz'
holdings = sparse.load_npz(path)
holdings.shape

#%%

## Cluster

transformer = MaxAbsScaler().fit(holdings) # fit does nothing.
holdings = transformer.transform(holdings)

pd.DataFrame(holdings.sum(0)).T.plot()

pd.DataFrame(holdings.sum(1)).plot()

#%%

print(multiprocessing.get_start_method())

multiprocessing.set_start_method('forkserver', force = True)
print(multiprocessing.get_start_method())
print("Changed start method")

#%%

print('Start kMeans...')
kmeans = KMeans(n_clusters = 4,
                verbose = True,
                n_init = 10, # Number of runs
                n_jobs= -1,
                random_state=0).fit(holdings[:10_000,:])