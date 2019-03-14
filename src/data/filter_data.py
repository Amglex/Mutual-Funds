import os
import sys

# add the 'src' directory as one where we can import modules
src_dir = os.path.join(os.getcwd(), os.pardir, 'src')
sys.path.append(src_dir)
from data.basic_functions import *

import numpy as np
import pandas as pd

from scipy import sparse


# For multiprocessing
import multiprocessing
from itertools import product

### Load the data files

#%%
data_path = '../data/raw/'

returns = load_data(data_path, 'monthly_returns')
summary = load_data(data_path, 'total_summary_new')  # Wo kommt das her ???

npz_path = '../data/interim/sparse_matrix_t.npz'
holdings = sparse.load_npz(npz_path)
holdings.shape

data_path = '../data/interim/'
holdings_summary = load_data(data_path, 'sparse_info_t')
holdings_summary.shape

path = '../data/raw/portno_map.feather'
portno_map = feather.read_dataframe(path)

#%%
## Drop duplicates in summary data

# To increase speed of matching obj_codes and other fund info to portfolios

print('Shape of summary before cleaning is: {:,} / {:,}'.format(summary.shape[0], summary.shape[1]))

# Delet not nas
summary_clean = summary[summary['crsp_portno'].notna()]

# Drop duplicates based on all but two columns
summary_clean = summary_clean.drop_duplicates(summary_clean.columns.difference(['crsp_fundno', 'fund_name']))

summary_clean.loc[:, 'crsp_fundno'] = pd.to_numeric(summary_clean.loc[:, 'crsp_fundno'], downcast='integer')
summary_clean.loc[:, 'crsp_portno'] = pd.to_numeric(summary_clean.loc[:, 'crsp_portno'], downcast='integer')

print('Shape of summary data after cleaning: {:,} / {:,}'.format(summary_clean.shape[0], summary_clean.shape[1]))

#%%
# Match obj code to portfolios

# TODO: Fund info flags like the index_fund_flag could in theory also be different in the fund_history
# Therefore a similar approach should also be used for those items
# In general portno fundno map also beginning and end times


def port_ID_to_port_info(fund_info):
	"""
	Used to merge the right obj_code and other fund info
	to each holdings_info row and therefore to each row of the sparse holdings matrix

	Input:
	- fund_info: Tuple consisting of the port_no (1st element) and the report_dt (2nd)

	Output:
	- Tuple of port_no, report_dt, index_fund_flag, et_flag and crsp_obj_cd.
		NaN if value is not available

	Attention:
	Depends on global variable summary to look up the values
	Must be renamed or changed in the function
	"""
	port_no = fund_info[0]
	report_dt = fund_info[1]
	mask = summary['crsp_portno'].values == port_no
	my_class = summary.loc[mask]

	my_class_n = my_class.loc[
		(my_class.begdt <= report_dt) &
		(my_class.enddt >= report_dt)]

	try:
		crsp_obj_cd = my_class_n['crsp_obj_cd'].values[0]
		index_fund_flag = my_class_n['index_fund_flag'].values[0]
		et_flag = my_class_n['et_flag'].values[0]

	except:
		crsp_obj_cd = np.nan
		index_fund_flag = np.nan
		et_flag = np.nan

	return (port_no, report_dt, index_fund_flag, et_flag, crsp_obj_cd)


### Multiprocessing

a = holdings_summary['port_no']
b = holdings_summary['date']

fund_info = list(zip(a, b))

% % time
with multiprocessing.Pool(processes=8) as pool:
	results = pool.map(port_ID_to_port_info, fund_info)

labels = ['port_no', 'report_dt', 'index_fund_flag', 'et_flag', 'crsp_obj_cd']
holdings_summary = pd.DataFrame.from_records(results, columns=labels)

holdings_summary.shape
holdings_summary[
	holdings_summary['crsp_obj_cd'].isna()].shape  # -> For some portfolios there is simly no row in fund_header

### Out of the roughly 730k portfolios, for 129k there is no fund header info available

holdings_summary.sample(10)

# Clean holdings_summary data

Do not delet rows
since
they
match
to
the
sparse
matrix

TODO: Replace
all
falgs
with proper categories

# Make the two flags categories and rename those categories accordingly
holdings_summary[['et_flag', 'index_fund_flag']] = holdings_summary[['et_flag', 'index_fund_flag']].astype('category')

et_mapper = {'F': 'ETF', 'N': 'ETN', np.nan: 'MF'}
holdings_summary['et_flag'] = holdings_summary['et_flag'].map(et_mapper)

index_flag_mapper = {'B': 'Index-based', 'D': 'Pure Index', 'E': 'Index enhanced', np.nan: 'MF'}
holdings_summary['index_fund_flag'] = holdings_summary['index_fund_flag'].map(index_flag_mapper)

# Creat new flag var 'mutual_fund' that is Y for Mutual Funds and N for other funds
holdings_summary.loc[(holdings_summary['index_fund_flag'] == 'MF') &
					 (holdings_summary['et_flag'] == 'MF'), 'mutual_fund'] = 'Y'
holdings_summary.loc[holdings_summary['mutual_fund'].isna(), 'mutual_fund'] = 'N'

# Creat new flag var 'sample' that is Y for those included and N for those not included
# , 'EDCL', 'EDCM', 'EDCS', 'EDCI'
selected_obj_codes = ('EDYG', 'EDYB', 'EDYH', 'EDYS', 'EDYI')

holdings_summary.loc[(holdings_summary['mutual_fund'] == 'Y') &
					 (holdings_summary['crsp_obj_cd'].isin(selected_obj_codes)), 'sample'] = 'Y'
holdings_summary.loc[holdings_summary['sample'].isna(), 'sample'] = 'N'

# Make the two new variables categorical
holdings_summary[['mutual_fund', 'sample']] = holdings_summary[['mutual_fund', 'sample']].astype('category')

holdings_summary['sample'].value_counts()

pd.crosstab(holdings_summary['et_flag'], holdings_summary['index_fund_flag'])

pd.crosstab(holdings_summary['mutual_fund'], holdings_summary['sample'])

#### 148k Funds in EDY
#### 202k Funds in EDC

# Save holdings_summary

path = '../data/interim/holdings_summary_total.feather'
# feather.write_dataframe(holdings_summary,path)

path = '../data/interim/holdings_summary_total.feather'
holdings_summary = feather.read_dataframe(path)

holdings.shape

holdings_summary.shape

### Add fund_no to holdings_summary

#### Fundo is not an integer for now but not that important -> TODO

portno_map_unique = portno_map.drop_duplicates(subset='crsp_portno')

#### Maybe must be modified since all but one associated fund_nos per portfolio are deleted

holdings_summary = holdings_summary.merge(portno_map_unique[['crsp_portno', 'crsp_fundno']], how='left',
										  left_on='port_no', right_on='crsp_portno')

holdings_summary.shape

mask = holdings_summary['crsp_fundno'].notna()
holdings_summary['crsp_fundno'] = holdings_summary.loc[mask, 'crsp_fundno'].astype(int)

new_order = [0, 8, 1, 2, 3, 4, 5, 6]
holdings_summary = holdings_summary[holdings_summary.columns[new_order]]

holdings_summary = holdings_summary.rename(columns={'crsp_fundno': 'fund_no'}, index=str)

# Take sample according to parameter

### Filter returns

unique_portno = holdings_summary[['fund_no']].drop_duplicates()

mask = returns['crsp_fundno'].isin(unique_portno['fund_no'])
returns_s = returns[mask]

returns.shape

returns_s.shape

#### From 7.2m to 4.4m return datapoints

### Filter holdings

#### Mask to filter out only those in the sample according to holdings_summary

mask = (holdings_summary['sample'] == 'Y')
np.sum(mask)

mask = (holdings_summary['sample'] == 'Y') & (holdings_summary['port_no'].notna())
np.sum(mask)

% % time
holdings_s = holdings[mask.values]
holdings_s

### Filter holdings summary

holdings_summary_s = holdings_summary[mask]

holdings_s.shape

holdings_summary_s.shape

### Save final cleaned and filtered data

#### Sparse matrix

path = '../data/processed/EDY/holdings_s'
sparse.save_npz(path, holdings_s)

#### Sparse info

path = '../data/processed/EDY/holdings_summary_s.feather'
feather.write_dataframe(holdings_summary_s, path)

#### Returns

path = '../data/processed/EDY/returns_s.feather'
feather.write_dataframe(returns_s, path)

## Take smaller sub_sub sample (Everything before specified year)
Makes
processing
faster

start_date = '2015-01-01'
end_date = '2018-01-01'

#### Holdings & holdings_summary

mask = (holdings_summary_s['report_dt'] > start_date) & (holdings_summary_s['report_dt'] < end_date)

holdings_s_s = holdings_s[mask.values]

holdings_summary_s_s = holdings_summary_s[mask]

#### Test

holdings_s_s.shape

holdings_summary_s_s.shape

#### Returns

mask = (returns_s['caldt'] > start_date) & (returns_s['caldt'] < end_date)
returns_s_s = returns_s[mask]

### Save final cleaned and filtered data

#### Sparse matrix

path = '../data/processed/EDY/holdings_s_s'
sparse.save_npz(path, holdings_s_s)

#### Sparse info

path = '../data/processed/EDY/holdings_summary_s_s.feather'
feather.write_dataframe(holdings_summary_s_s, path)

#### Returns

path = '../data/processed/EDY/returns_s_s.feather'
feather.write_dataframe(returns_s_s, path)

## Delet columns with little to no information

col_sums = pd.DataFrame(holdings.sum(0)).T.values

sum(col_sums < 1)

