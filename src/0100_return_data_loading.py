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
    FROM monthly_returns
    '''
)

data_raw_df = data_raw_df.copy()

print('SQL successful')


print(data_raw_df.head())

# path = 'data/data_df.feather'
path = '../data/monthly_returns.feather'

feather.write_dataframe(data_raw_df, path)

print("Successfully saved data")
