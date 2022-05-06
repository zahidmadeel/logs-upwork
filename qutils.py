import csv
import os
import pandas as pd

def clean_log(log_json):
	entries_to_remove = ("v", "msg", "level", "pid", "target", "line","file")
	for k in entries_to_remove:
	    log_json.pop(k, None)	
	if log_json.get("timestamp"):
		log_json["timestamp"] = int(log_json["timestamp"])
	tribe_id = log_json["tribe_id"]
	clan_id = log_json["clan_id"]
	node_id = log_json["node_id"]
	mix_key = "{}-{}-{}".format(tribe_id, clan_id, node_id)
	log_json["node_key"] = mix_key
	# print("clean_log: {}".format(log_json))
	return log_json

def create_csv_file(file_name, headers):
	header = ['batch', 'node', 'time_diff']
	with open("./outputs/" + file_name, 'w+', encoding='UTF8') as f:
		writer = csv.writer(f)
		writer.writerow(headers)
	return file_name


def write_row_to_csv_file(file_name, rows):
	header = ['batch', 'node', 'time_diff']
	with open("./outputs/" + file_name, 'w+', encoding='UTF8') as f:
		writer = csv.writer(f)
		writer.writerows(rows)

'''This function is meant to replace the function write_to_csv_file that does not input column names of the csv file.
This function creates a pandas dataframe and let the pandas libaray worry about writing csv from list '''
def write_result_to_csv_file(file_name, data, column_names):
	df = pd.DataFrame(data, columns=column_names)
	df.to_csv(os.path.join('outputs', file_name), index=None)
