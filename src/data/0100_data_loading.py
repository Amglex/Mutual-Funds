# Description
#
#
#

import wrds
import feather

# Connect to DB
db = wrds.Connection(wrds_username='amglex')
print('Successfully connected')


######################
# Query the data
######################


print('Start downloading data ...')

# SQL Query
data_raw_df = db.raw_sql(
    '''
    SELECT *
    FROM crsp_portno_map
    '''
)

print('SQL successful')

print(data_raw_df.shape)

print(data_raw_df.dtypes)

print(data_raw_df.head())

path = '../../data/raw/portno_map.feather'

feather.write_dataframe(data_raw_df, path)

print("Successfully saved data")
