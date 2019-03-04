import config
import feather


def load_data(file_name):
	"""
	Wrapper for feather.read_dataframe
	Takes one arguments:
	- The filename
	Relies on a data_path variable which must be specified at the beginning
	:rtype: object
	"""

	path = config.data_path + file_name + '.feather'
	data = feather.read_dataframe(path)
	return data


def save_data(file, file_name):
	"""
	Wrapper for feather.write_dataframe
	Takes two arguments:
	- The file
	- Under what name the file should be saved
	Relies on a data_path variable which must be specified at the beginning
	"""
	path = config.data_path + file_name + '.feather'
	feather.write_dataframe(file, path)
