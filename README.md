# dkafka
kafka cloudevent integration


# Usage

* Compile with `go install -v ./cmd/dkafka`
* Run with:
```
  # assuming a dfuse instance with blocks running on localhost:9000
  # assuming a kafka instance running on localhost:9092 without auth

  dkafka publish \
     --dfuse-firehose-grpc-addr=localhost:9000 \
     --start-block-num=2  \ 
     --stop-block-num=180 \
     --dfuse-firehose-include-expr='action!="onblock"' \
     --kafka-endpoints=localhost:9092 \
     --kafka-topic=dkafka \
     --kafka-cursor-topic=_dkafka_cursor \
     --kafka-cursor-partition=0
```
 
# CEL expression language

* Reference: https://github.com/google/cel-spec/blob/master/doc/langdef.md

* The following flags on dkafka accept CEL expressions to generate a string or an array of strings:
  * *--event-type-expr*  "CEL" --> `string`
  * *--event-keys-expr* "CEL" -->  `[array,of,strings]`
  * *--event-extensions-expr* : "key1:CEL1[,key2:CEL2...]" where each CEL expression --> `string`

* the following names are available to be resolved from the EOS blocks, transactions, traces and actions.
  * `receiver`: receiver account, should be the same as the `account` unless it is a notification, ex: `eosio.token`, `johndoe12345`.   
  * `account`: namespace of the action, ex: `eosio.token`
  * `action`: name of the action, ex: `transfer`
  * `block_num`: block number, ex: 234522
  * `block_id`: unique ID for the block, ex: `0002332afef44aad7d8f49374398436349372fcdb`
  * `block_time`: timestamp of the block
  * `step`: one of: `NEW`,`UNDO`,`IRREVERSIBLE`
  * `transaction_id`: unique ID for the transaction, ex: `6d0aae37ab3b81b6783b877f2d54d4708f9f137cc6b23374641be362ff010803`
  * `transaction_index`: position of that transaction in the block (ex: 5)
  * `global_seq`: unique sequence number of that action in the chain (ex: 4354352435)
  * `execution_index`: position of that action in the transaction, ordered by execution
  * `data`: map[string]Any corresponding to the params given to the action
  * `auth`: array of strings corresponding to the authorizations given to the action (who signed it?)
  * `input`: bool, if true, the action is top-level (declared in the transaction, not a consequence of another action)
  * `notif`: bool, if true, the action is a 'notification' (receiver!=account)
  * `scheduled`: bool, if true, the action was scheduled (delayed or deferred)
  * `trx_action_count`: number of actions within that transaction
  * top5`_trx_actors`: array of the 5 most recurrent actors in a transaction (useful for big transactions with lots of actions)

