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
    SELECT crsp_portno, report_dt, percent_tna, nbr_shares, market_val, crsp_company_key
    FROM holdings
    '''
)

print('SQL successful')

data_raw_df['crsp_portno'] = data_raw_df['crsp_portno'].astype(int)
data_raw_df['crsp_company_key'] = data_raw_df['crsp_company_key'].astype(int)

print(data_raw_df.dtypes)

print(data_raw_df.head())

path = '../../data/raw/holdings_total.feather'

feather.write_dataframe(data_raw_df, path)

print("Successfully saved data")
