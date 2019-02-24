# Description


import feather
import numpy as np
import pandas as pd

from sklearn.preprocessing import OneHotEncoder, LabelEncoder
from scipy import sparse

######################
# Load the data
######################

path = 'data/data_df.feather'
data_df = feather.read_dataframe(path)

print(data_df.shape)

print('Raw data successfully loaded')

######################
# Encode
######################

data_for_encoding_df = data_df.loc[:, ['port_ID', 'crsp_company_key', 'percent_tna']]

label_enc = LabelEncoder()

one_hot_enc = OneHotEncoder(
    categories='auto',
    sparse='True'
)

print(data_for_encoding_df.crsp_company_key.values.reshape(-1, 1))



encoded_dummies = one_hot_enc.fit_transform(data_for_encoding_df.crsp_company_key.values.reshape(-1, 1), )

# Gen groups out of port_ID
groups = data_for_encoding_df.port_ID

groups = label_enc.fit_transform(groups)

# Grouping

groups = np.array(groups)

keys = groups.flatten()
rows = encoded_dummies.indptr[1:]
cols = encoded_dummies.indices
data = encoded_dummies.data

sparse_data = (
    np.vstack((keys, rows, cols, data))  # combine all vectors
    .T  # Transpose to long format
    .astype(int)  # Change to int
)

# Sparse_data_pd = pd.DataFrame(sparse_data)

sparse_grouped = sparse.coo_matrix((sparse_data[:, 3], (sparse_data[:, 0], sparse_data[:, 2])))
sparse_grouped = sparse.csr_matrix(sparse_grouped)

# Save Matrix

path = 'data/data_sparse_more.npz'
sparse.save_npz(path, sparse_grouped)

print('Sparse matrix successfully saved')

######################
# Gen Class per port_ID
######################

Y_df = data_df.loc[:, ['crsp_portno', 'report_dt', 'port_ID', 'crsp_obj_cd']]
Y_df = Y_df.assign(group=groups)

Y_grouped_df = Y_df.groupby('group').head(1)

print(Y_grouped_df.head())


# Save other information per portfolio
path = 'data/info_df.feather'
feather.write_dataframe(Y_grouped_df, path)


