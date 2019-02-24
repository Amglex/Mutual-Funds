# Description
#
#
#

import feather
import pandas as pd

######################
# Load the data
######################

path = 'data/data_raw_df.feather'
data_df = feather.read_dataframe(path)

print('Raw data successfully loaded')


###############
# Delete rows
###############

print('Shape before rows are removed: ', data_df.shape)


# Delete non-unique rows
labels = ['crsp_portno', 'report_dt', 'crsp_company_key']
data_df = data_df.drop_duplicates(labels)


# Delete rows with NaNs
data_df.dropna(inplace=True)

# Delete rows with rare companies
# Calc occurrence per company
comp_freq_df = (data_df[['crsp_company_key', 'crsp_portno']]
                .groupby(by=['crsp_company_key'])
                .count()
                )

# Select the crsp_company_keys which should be deleted
top_n_to_delete = 3
comp_to_delete = comp_freq_df[comp_freq_df.crsp_portno <= top_n_to_delete].index

# Delete the respective rows
data_df = data_df[~data_df.crsp_company_key.isin(comp_to_delete)]


print('Shape after rows are removed: ', data_df.shape)

######################
# Manipulate the data
######################

# To numeric
to_numeric_columns = ['crsp_portno', 'crsp_company_key', 'crsp_fundno']
data_df[to_numeric_columns] = data_df[to_numeric_columns].astype(int)

# To date
data_df['report_dt'] = pd.to_datetime(data_df['report_dt'])

# To category
data_df['crsp_obj_cd'] = data_df['crsp_obj_cd'].astype('category')

# To category
# data_df.percent_tna = 1  # Change all percent_tna to 1 -> Simplification -> can be changed later


# Add unique identifier
print('Gen unique port_ID')
data_df = (data_df
           .assign(port_ID=data_df['crsp_portno'].astype(str) + '-' + data_df['report_dt'].astype(str))
           .sort_values('port_ID')
           )

print('Finished')


######################
# Save
######################

path = 'data/data_df.feather'
feather.write_dataframe(data_df, path)

print("Successfully saved data")

print(data_df.head())
