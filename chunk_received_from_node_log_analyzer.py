import pandas as pd
import json
import qutils

path_to_chunk_received_from_node_log = "./parsed_logs/BATCH_CHUNK_RECEIVED_FROM_NODE.json"
with open(path_to_chunk_received_from_node_log) as f:
    lst = []
    for line in f:
        try:
            line_data = json.loads(line)
        except:
            continue
        chunk_data = qutils.clean_log(line_data)
        lst.append(chunk_data)



df = pd.DataFrame(lst)

print(df.head())