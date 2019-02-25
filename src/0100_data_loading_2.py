# Description
#
#
#

import wrds
import feather
import pandas as pd

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
    SELECT hdr.crsp_fundno, hdr.crsp_portno, 
        fund_name, first_offer_dt, index_fund_flag, et_flag, 
        begdt, enddt, 
        crsp_obj_cd, lipper_class, lipper_class_name, lipper_obj_cd
    FROM fund_hdr hdr
    FULL JOIN fund_style style 
    ON hdr.crsp_fundno = style.crsp_fundno 
    LIMIT 100
    '''
)

print('SQL successful')


with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(data_raw_df.head())

print(data_raw_df)

print(data_raw_df.shape)

# path = 'data/data_df.feather'
path = 'data/total_summary.feather'

feather.write_dataframe(data_raw_df, path)

print("Successfully saved data")