import pandas as pd
import json
import qutils
import sys


def get_dataframe():
    path_to_chunk_received_from_node_log = "./parsed_logs/BATCH_CHUNK_RECEIVED_FROM_NODE.json"
    with open(path_to_chunk_received_from_node_log) as f:
        lst, lcount = [], 1
        for line in f:
            # print(lcount)
            try:
                line_data = json.loads(line)
            except:
                continue
            chunk_data = qutils.clean_log(line_data)
            lst.append(chunk_data)
            # lcount += 1
    df = pd.DataFrame(lst)
    dfs = df[['node_key', 'batch_id', 'chunk_index', 'chunk_hash', 'timestamp']]
    return dfs

def get_chunk_data(batch_id):
    num_chunks, path_to_chunk_creation_at_leader_log = 0, "./parsed_logs/BATCH_CHUNK_CREATION_AT_LEADER.json"
    with open(path_to_chunk_creation_at_leader_log) as f:
        for line in f:
            try:
                line_data = json.loads(line)
            except:
                continue
            chunk_data = qutils.clean_log(line_data)

            if chunk_data['batch_id'] == batch_id:
                num_chunks += 1

    return num_chunks


def main(batch_id, node_key):

    num_chunks, dfs = get_chunk_data(batch_id), get_dataframe().astype({'chunk_index':'int'})
    first_chunk, middle_chunk = 0,  int(num_chunks/2)  + 1
    df_t1 = dfs[(dfs.batch_id == batch_id) & (dfs.node_key == node_key)].sort_values(['batch_id', 'node_key', 'chunk_index', 'timestamp'])
    print(df_t1)
    first_row, middle_row = df_t1[df_t1['chunk_index'] == first_chunk].iloc[0], df_t1[df_t1['chunk_index'] == middle_chunk].iloc[0]
    time_first, time_middle = first_row['timestamp'], middle_row['timestamp']
    print(f'First chunk index is {first_chunk}, middle chunk index is {middle_chunk} and total number of chunks are {num_chunks}. The '
          f'time difference between arrival of middle and first is {time_middle - time_first}')

    df_t1.to_csv(f"outputs/logs_{batch_id}_{node_key}.csv", index=None)


if __name__ == "__main__":
    batch_number = sys.argv[1]
    node_key = sys.argv[2]
    # batch_number, node_key = '9abc5eba9a68b2aaedc860c8c5b9e69892dfe27719f1fa14e30abbabbfaef340', '0-0-4'
    main(batch_number, node_key)
