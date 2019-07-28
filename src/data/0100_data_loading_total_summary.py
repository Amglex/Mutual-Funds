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

#SQL Query: Summary table //// OLD WAY
# TODO Look ahead because of per_com?

data_raw_df = db.raw_sql(
    '''
    SELECT hdr.crsp_fundno, hdr.crsp_portno, hdr.fund_name,
        first_offer_dt, index_fund_flag, et_flag,
        begdt, enddt, lipper_class, avrcs
    FROM fund_hdr hdr
    FULL JOIN fund_style style
    ON hdr.crsp_fundno = style.crsp_fundno
    
    LEFT JOIN   
        (SELECT distinct 
            crsp_fundno, sum(per_com)/count(per_com) as avrcs
        FROM fund_summary 
        GROUP BY crsp_fundno) b
    ON style.crsp_fundno = b.crsp_fundno;
    '''
)

print('SQL successful')

print(data_raw_df.shape)

path = '../../data/raw/total_summary_new.feather'
feather.write_dataframe(data_raw_df, path)

print("Successfully saved data")