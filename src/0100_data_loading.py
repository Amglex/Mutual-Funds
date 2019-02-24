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
    SELECT holdings.crsp_portno, report_dt , percent_tna , crsp_company_key, fund_hdr.crsp_fundno , crsp_obj_cd 
    FROM holdings, fund_hdr , fund_style 
    WHERE  holdings.crsp_portno = fund_hdr.crsp_portno 
    AND fund_hdr.crsp_fundno = fund_style.crsp_fundno 
    AND report_dt > '2018-05-01'
    '''
)

print('SQL successful')


print(data_raw_df.head())

# path = 'data/data_df.feather'
path = 'data/2018_data_df.feather'

feather.write_dataframe(data_raw_df, path)

print("Successfully saved data")