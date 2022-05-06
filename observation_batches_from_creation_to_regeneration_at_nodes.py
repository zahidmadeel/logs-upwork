import os
import csv
import sys
import json
import qutils
import datetime
from collections import defaultdict
import pandas as pd

PATH_TO_LOGS = '.'
BATCH_CREATION_TIME = {}
BATCH_CREATION_TIME_OREDERED = [];
BATCH_REGENERATION_TIME = {}

def parse_data(sim_number):
	BATCHES_COUNTER = 0
	batches = [];
	path_to_log = "./parsed_logs/batch_logs.json"
	line_counter = 0
	with open(path_to_log) as f:
		for line in f:
			line_counter +=1
			try:
				line_data = json.loads(line)
			except:
				continue
				
			if "BATCH_CREATED" in line:
				BATCHES_COUNTER +=1
				batch_data = line_data
				batch_data = qutils.clean_log(batch_data)
				BATCH_CREATION_TIME [ batch_data["batch_id"] ] = batch_data 

			if "BATCH_REGENERATE_IN_NODES" in line:
				batch_data2 = line_data
				batch_data2 = qutils.clean_log(batch_data2)
				# print("batch_data2[node key] : {}".format(batch_data2["node_key"]))
				batch_id = batch_data2["batch_id"]
				node_key2 = batch_data2["node_key"]
				if BATCH_REGENERATION_TIME.get(batch_id):
					BATCH_REGENERATION_TIME[ batch_id ][node_key2] = batch_data2 
				else:
					BATCH_REGENERATION_TIME[ batch_id ] = {}
					BATCH_REGENERATION_TIME[ batch_id ][node_key2] = batch_data2
					

	print("BATCHES_COUNTER: {}".format(BATCHES_COUNTER))
	return batches

def main(sim_number):
	batches = parse_data(sim_number)

	rows = [["batch_id","transactions", "node_key", "time to regenerate"]]
	for batch_id in BATCH_CREATION_TIME:
		print("batch_id: {}".format(batch_id))
		creation_time = BATCH_CREATION_TIME[batch_id]["timestamp"]
		transactions = BATCH_CREATION_TIME[batch_id]["number_of_transaction"]
		for node_key in BATCH_REGENERATION_TIME[ batch_id ]:
			print("node_key: {}".format(node_key))
			regeneration_data = BATCH_REGENERATION_TIME[batch_id].get(node_key) 
			regeneration_time = regeneration_data["timestamp"]
			if regeneration_time:
				time_diff = regeneration_time - creation_time
				new_row = [batch_id, transactions,node_key,time_diff]
				rows.append(new_row)
	qutils.write_row_to_csv_file("batches_from_creation_to_regeneration{}.csv".format(sim_number), rows)

if __name__ == "__main__":
	# sim_number = sys.argv[1]
	main('abc')
