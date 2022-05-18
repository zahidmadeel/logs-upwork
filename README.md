
# Instructions

Your code output needs to be a CSV output file saved under outpus folder.

1. Extract the content of the zip file into parsed_logs folder
2. Run code with 4 digit SIM_NUMBER (for example 0001)
```
python3.8 observation_batches_from_creation_to_regeneration_at_nodes.py SIM_NUMBER
```
Added the code to analyze `BATCH_CHUNK_RECEIVED_FROM_NODE` logs. The file can be executed
using the following command
```
python chunk_received_from_node_log_analyzer.py batch_id node_key
```
Where `batch_id` and `node_key` are passed as input. so far, they have been used to
investigate the scenario where the arrival difference of middle chunk and the first chunk
is negative. The script above also creates a `csv` file in `outputs`
directory containing received chunks for the given `batch_id` and`node_key`
# Log List

 - BATCH_CREATED
 - BATCH_REGENERATE_IN_NODES
 - BATCH_CHUNK_CREATION_AT_LEADER
 - BATCH_CHUNK_SENT_TO_NODE
 - BATCH_CHUNK_RECEIVED_FROM_NODE
 - TRANSACTION_RECEIVED_AT_NODE
 - TRANSACTION_SIGNATURE_VERIFIED
 - TRANSACTION_SENT_TO_LEADER
 - TRANSACTION_RECEIVED_BY_LEADER
 - TRANSACTION_ENTERED_THE_POOL
 - TRANSACTION_REMOVED_FROM_THE_POOL
 - TRANSACTION_WAS_ENTERED_TO_BATCH
 - TRANSACTION_POOL_HIT_SIZE_LIMIT
 - TRANSACTION_POOL_HIT_TIME_LIMIT

All logs has timestamp, tribe_id, clan_id, node_id
Batches has batch_id
Chunks has batch_id and chunk_hash