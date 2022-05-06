
# Instructions

Your code output needs to be a CSV output file saved under outpus folder.

1. Extract the content of the zip file into parsed_logs folder
2. Run code with 4 digit SIM_NUMBER (for example 0001)
```
python3.8 observation_batches_from_creation_to_regeneration_at_nodes.py SIM_NUMBER
```

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