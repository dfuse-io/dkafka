#!/bin/bash
make clean test build
for i in {1..1000}
do
   echo "Run tables $i times"
   make forward-stop
   make forward
   ./build/dkafka cdc tables \
		--dfuse-firehose-grpc-addr=localhost:9000 \
		--abicodec-grpc-addr=localhost:9001 \
		--kafka-endpoints=kafka-roro-361b174f-romain-0052.aivencloud.com:22123 \
      --kafka-ssl-auth=true \
      --kafka-ssl-enable=true \
      --kafka-ssl-ca-file=./ca.pem \
      --kafka-ssl-client-cert-file=./client.crt.pem \
      --kafka-ssl-client-key-file=./client.key.pem \
      --kafka-compression-type=none \
      --kafka-compression-level=-1 \
      --kafka-message-max-bytes=1000000 \
      --delay-between-commits=600s \
      --executed \
      --codec=avro \
      --batch-mode=false \
      --event-source=dkafka-data-tables \
      --table-name=* \
      --start-block-num=0 \
      --kafka-topic=io.dkafka.data.eosio.oracle.tables.v1 \
      --namespace=io.dkafka.data.eosio.oracle.tables.v1 \
        eosio.oracle
done