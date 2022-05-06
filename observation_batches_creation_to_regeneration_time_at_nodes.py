import os
import csv
import sys
import json
import qutils
import re
import datetime
from collections import defaultdict
import pandas as pd

PATH_TO_LOGS = '.'
BATCH_CREATION_TIME = {}
BATCH_CREATION_TIME_OREDERED = [];
BATCH_REGENERATION_TIME = {}


def parse_data(sim_number):
    batch_dic = {}
    path_to_log = "./parsed_logs/batch_logs.json"
    with open(path_to_log) as f:
        for line in f:
            try:
                line_data = json.loads(line)
            except:
                continue

            if "BATCH_CREATED" in line:
                batch_data = line_data
                batch_data = qutils.clean_log(batch_data)
                if not batch_dic.get(batch_data['batch_id']):
                    batch_dic[batch_data['batch_id']] = {}
                batch_dic[batch_data['batch_id']]['batch_master'] = batch_data

            if "BATCH_CHUNK_CREATION_AT_LEADER" in line:
                chunk_data = qutils.clean_log(line_data)
                if not batch_dic.get(chunk_data['batch_id']):
                    batch_dic[chunk_data['batch_id']] = {}
                if not batch_dic[chunk_data['batch_id']].get('num_chunks'):
                    batch_dic[chunk_data['batch_id']]['num_chunks'] = 0
                batch_dic[chunk_data['batch_id']]['num_chunks'] += 1

            if 'BATCH_CHUNK_RECEIVED_FROM_NODE' in line:
                chunk_data = qutils.clean_log(line_data)
                if not batch_dic.get(chunk_data['batch_id']):
                    batch_dic[chunk_data['batch_id']] = {}
                if not batch_dic[chunk_data['batch_id']].get(chunk_data['node_key']):
                    batch_dic[chunk_data['batch_id']][chunk_data['node_key']] = {}
                if not batch_dic[chunk_data['batch_id']][chunk_data['node_key']].get(int(chunk_data['chunk_index'])):
                    batch_dic[chunk_data['batch_id']][chunk_data['node_key']][int(chunk_data['chunk_index'])] = {
                        'timestamp': chunk_data['timestamp'], 'chunk_hash': chunk_data['chunk_hash'],
                        'chunk_index': chunk_data['chunk_index']}
                else:
                    existing_chunk = batch_dic[chunk_data['batch_id']][chunk_data['node_key']][
                        int(chunk_data['chunk_index'])]
                    existing_ts, current_ts = int(existing_chunk['timestamp']), int(chunk_data['timestamp'])
                    if current_ts < existing_ts:
                        batch_dic[chunk_data['batch_id']][chunk_data['node_key']][int(chunk_data['chunk_index'])] = {
                            'timestamp': chunk_data['timestamp'], 'chunk_hash': chunk_data['chunk_hash'],
                            'chunk_index': chunk_data['chunk_index']}

    return batch_dic


def parse_data_multiple_files(sim_number):
    batch_dic = {}
    path_to_batch_created_log = "./parsed_logs/BATCH_CREATED.json"
    path_to_chunk_creation_at_leader_log = "./parsed_logs/BATCH_CHUNK_CREATION_AT_LEADER.json"
    path_to_chunk_received_from_node_log = "./parsed_logs/BATCH_CHUNK_RECEIVED_FROM_NODE.json"
    with open(path_to_batch_created_log) as f:
        for line in f:
            try:
                line_data = json.loads(line)
            except:
                continue
            batch_data = line_data
            batch_data = qutils.clean_log(batch_data)
            if not batch_dic.get(batch_data['batch_id']):
                batch_dic[batch_data['batch_id']] = {}
            batch_dic[batch_data['batch_id']]['batch_master'] = batch_data

    with open(path_to_chunk_creation_at_leader_log) as f:
        for line in f:
            try:
                line_data = json.loads(line)
            except:
                continue
            chunk_data = qutils.clean_log(line_data)
            if not batch_dic.get(chunk_data['batch_id']):
                batch_dic[chunk_data['batch_id']] = {}
            if not batch_dic[chunk_data['batch_id']].get('num_chunks'):
                batch_dic[chunk_data['batch_id']]['num_chunks'] = 0
            batch_dic[chunk_data['batch_id']]['num_chunks'] += 1
    with open(path_to_chunk_received_from_node_log) as f:
        for line in f:
            try:
                line_data = json.loads(line)
            except:
                continue
            chunk_data = qutils.clean_log(line_data)
            if not batch_dic.get(chunk_data['batch_id']):
                batch_dic[chunk_data['batch_id']] = {}
            if not batch_dic[chunk_data['batch_id']].get(chunk_data['node_key']):
                batch_dic[chunk_data['batch_id']][chunk_data['node_key']] = {}
            if not batch_dic[chunk_data['batch_id']][chunk_data['node_key']].get(int(chunk_data['chunk_index'])):
                batch_dic[chunk_data['batch_id']][chunk_data['node_key']][int(chunk_data['chunk_index'])] = {
                    'timestamp': chunk_data['timestamp'], 'chunk_hash': chunk_data['chunk_hash'],
                    'chunk_index': chunk_data['chunk_index']}
            else:
                existing_chunk = batch_dic[chunk_data['batch_id']][chunk_data['node_key']][
                    int(chunk_data['chunk_index'])]
                existing_ts, current_ts = int(existing_chunk['timestamp']), int(chunk_data['timestamp'])
                if current_ts < existing_ts:
                    batch_dic[chunk_data['batch_id']][chunk_data['node_key']][int(chunk_data['chunk_index'])] = {
                        'timestamp': chunk_data['timestamp'], 'chunk_hash': chunk_data['chunk_hash'],
                        'chunk_index': chunk_data['chunk_index']}

    return batch_dic


def prepare_batch_time_data_for_saving(sim_number):
    batch_dic = parse_data_multiple_files(sim_number)
    data, issues, i = [], [], 0
    for k in batch_dic.keys():
        if 'num_chunks' not in batch_dic[k].keys():
            issues.append([k, 'BATCH_CHUNK_CREATION_AT_LEADER_NOT_FOUND'])
        else:
            n = batch_dic[k]['num_chunks']
            for v in batch_dic[k].keys():
                if v not in ['num_chunks', 'batch_master']:
                    mid = int(n / 2) + 1
                    if 0 not in batch_dic[k][v].keys():
                        issues.append([k, 'FIRST_CHUNK_AT_0_NOT_FOUND_FOR_NODE_{}'.format(v)])
                    elif mid not in batch_dic[k][v].keys():
                        issues.append([k, 'MIDDLE_CHUNK_AT_{}_NOT_FOUND_FOR_NODE_{}'.format(mid, v)])
                    else:
                        chunk_1, chunk_m = batch_dic[k][v][0], batch_dic[k][v][mid]
                        time, chunk1_hash, chunkm_hash = chunk_m['timestamp'] - chunk_1['timestamp'], chunk_1[
                            'chunk_hash'], chunk_m['chunk_hash']
                        data.append([k, v, chunk1_hash, chunkm_hash, time])

    print(issues)
    print(data)
    return data, issues



def main(sim_number):

    data, issues = prepare_batch_time_data_for_saving(sim_number)

    qutils.write_result_to_csv_file("batches_from_creation_to_regeneration_time-{}.csv".format(sim_number), data, ["batch_id", "node_key", "hash_1", "hash_m", "time"])
    qutils.write_result_to_csv_file("batches_from_creation_to_regeneration_time-issues-{}.csv".format(sim_number), issues,
                                    ["batch_id", "issue_description"])


if __name__ == "__main__":
    sim_number = sys.argv[1]
    main(sim_number)
