# Test performance of kmeans algorithm with different start methods
# Problem of kmeans not running properly in parallel on macos


from scipy import sparse
from sklearn.cluster import KMeans
from time import time
import multiprocessing

#%%

print(multiprocessing.get_start_method())

#%%
# Load Data
path = 'data/processed/EDY/holdings_s.npz'
holdings = sparse.load_npz(path)

data = holdings[0:1_000]
print("Loaded data ({})".format(data.shape))

#%%
kmeans = KMeans(n_clusters=4,
                n_init = 100,  # Number of runs
                n_jobs= -1,
                random_state=0)


#%%
# Test without

print('Start kMeans without forkserver')

t1= time()
test1 = kmeans.fit(data)
t2= time()

diff1= t2 -t1
print('finished in {}'.format(diff1))

#%%
multiprocessing.set_start_method('forkserver', force = True)
print(multiprocessing.get_start_method())
print("Changed start method")

#%%
print('Start kMeans with forkserver')

t3 = time()
test2 = kmeans.fit(data)
t4 = time()

diff2 = t4 - t3
print('finished in {}'.format(diff2))

